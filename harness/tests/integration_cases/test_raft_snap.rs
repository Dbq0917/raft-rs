// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use crate::test_util::*;
use harness::Network;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::{
    default_logger, Config, Error, GetEntriesContext, ProgressState, Raft, RaftState, Result,
    Storage, INVALID_INDEX,
};

fn testing_snap() -> Snapshot {
    new_snapshot(11, 11, vec![1, 2])
}

#[derive(Clone)]
struct FixedSnapshotStorage {
    inner: MemStorage,
    data: Arc<Vec<u8>>,
}

impl Storage for FixedSnapshotStorage {
    fn initial_state(&self) -> Result<RaftState> {
        self.inner.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> Result<Vec<Entry>> {
        self.inner.entries(low, high, max_size, context)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        self.inner.term(idx)
    }

    fn first_index(&self) -> Result<u64> {
        self.inner.first_index()
    }

    fn last_index(&self) -> Result<u64> {
        self.inner.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot> {
        let mut snapshot = self.inner.snapshot(request_index, to)?;
        snapshot.data = self.data.as_slice().to_vec().into();
        Ok(snapshot)
    }
}

fn take_single_message<T: Storage>(raft: &mut Raft<T>) -> Message {
    assert_eq!(raft.msgs.len(), 1, "messages: {:?}", raft.msgs);
    raft.msgs.remove(0)
}

#[test]
fn test_snapshot_transfer_resumes_from_confirmed_offset() {
    let l = default_logger();
    let payload = b"0123456789".to_vec();
    let store = MemStorage::new_with_conf_state((vec![1, 2], vec![]));
    store
        .wl()
        .apply_snapshot(new_snapshot(11, 11, vec![1, 2]))
        .unwrap();
    let storage = FixedSnapshotStorage {
        inner: store,
        data: Arc::new(payload.clone()),
    };
    let mut leader = Raft::new(
        &Config {
            id: 1,
            election_tick: 10,
            heartbeat_tick: 1,
            max_snapshot_chunk_size: 4,
            ..Default::default()
        },
        storage,
        &l,
    )
    .unwrap();
    leader.become_candidate();
    leader.become_leader();
    {
        let progress = leader.mut_prs().get_mut(2).unwrap();
        progress.next_idx = 1;
        progress.recent_active = true;
    }

    let mut follower = Raft::new(
        &Config {
            id: 2,
            election_tick: 10,
            heartbeat_tick: 1,
            max_snapshot_chunk_size: 4,
            ..Default::default()
        },
        MemStorage::new_with_conf_state((vec![1, 2], vec![])),
        &l,
    )
    .unwrap();

    leader.send_append(2);
    let first = take_single_message(&mut leader);
    assert_eq!(first.snapshot_offset, 0);
    assert_eq!(first.snapshot_size, payload.len() as u64);
    assert_eq!(first.get_snapshot().get_data(), &payload[..4]);

    follower.step(first).unwrap();
    let first_ack = take_single_message(&mut follower);
    assert_eq!(first_ack.get_msg_type(), MessageType::MsgSnapshotAck);
    assert_eq!(first_ack.snapshot_offset, 4);
    assert!(!first_ack.reject);

    leader.step(first_ack).unwrap();
    let second = take_single_message(&mut leader);
    assert_eq!(second.snapshot_offset, 4);

    // The second chunk is lost. The transport reports the failure, and the
    // next heartbeat makes the leader retry from the last confirmed offset.
    let mut failure = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    failure.reject = true;
    leader.step(failure).unwrap();
    leader
        .step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0))
        .unwrap();
    let resumed = take_single_message(&mut leader);
    assert_eq!(resumed.snapshot_offset, 4);
    assert_eq!(resumed.get_snapshot().get_data(), &payload[4..8]);

    follower.step(resumed).unwrap();
    let second_ack = take_single_message(&mut follower);
    assert_eq!(second_ack.snapshot_offset, 8);
    leader.step(second_ack).unwrap();
    let third = take_single_message(&mut leader);
    assert_eq!(third.snapshot_offset, 8);

    // The follower restarts and loses its partial snapshot. It must not accept
    // a chunk that starts above zero; it asks the leader to restart instead.
    let mut restarted = Raft::new(
        &Config {
            id: 2,
            election_tick: 10,
            heartbeat_tick: 1,
            max_snapshot_chunk_size: 4,
            ..Default::default()
        },
        MemStorage::new_with_conf_state((vec![1, 2], vec![])),
        &l,
    )
    .unwrap();
    restarted.step(third).unwrap();
    let restart_ack = take_single_message(&mut restarted);
    assert_eq!(restart_ack.snapshot_offset, 0);
    assert!(restart_ack.reject);

    leader.step(restart_ack).unwrap();
    let rewind = take_single_message(&mut leader);
    assert_eq!(rewind.snapshot_offset, 0);

    restarted.step(rewind).unwrap();
    let ack = take_single_message(&mut restarted);
    leader.step(ack).unwrap();
    let next = take_single_message(&mut leader);
    restarted.step(next).unwrap();
    let ack = take_single_message(&mut restarted);
    leader.step(ack).unwrap();
    let last = take_single_message(&mut leader);
    restarted.step(last).unwrap();
    let final_ack = restarted
        .msgs
        .iter()
        .find(|m| m.get_msg_type() == MessageType::MsgSnapshotAck)
        .cloned()
        .unwrap();
    assert_eq!(final_ack.snapshot_offset, payload.len() as u64);

    leader.step(final_ack).unwrap();
    let progress = leader.prs().get(2).unwrap();
    assert_eq!(progress.state, ProgressState::Probe);
    assert_eq!(progress.pending_snapshot, 0);
    assert_eq!(progress.pending_snapshot_offset, 0);
    assert_eq!(restarted.snap().unwrap().get_data(), payload.as_slice());
}

#[test]
fn test_sending_snapshot_set_pending_snapshot() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 1, so that
    // node 1 needs a snapshot
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index();

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    {
        let voter_2 = sm.prs().get(2).unwrap();
        m.index = voter_2.next_idx - 1;
        m.reject = true;
    };
    sm.step(m).expect("");
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 11);
}

#[test]
fn test_pending_snapshot_pause_replication() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    sm.step(new_message(1, 1, MessageType::MsgPropose, 1))
        .expect("");
    let msgs = sm.read_messages();
    assert!(msgs.is_empty());
}

#[test]
fn test_snapshot_failure() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.reject = true;
    sm.step(m).expect("");
    let voter_2 = sm.prs().get(2).unwrap();
    assert_eq!(voter_2.pending_snapshot, 0);
    assert_eq!(voter_2.next_idx, 1);
    assert!(voter_2.paused);
}

#[test]
fn test_snapshot_succeed() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.reject = false;
    sm.step(m).expect("");
    let voter_2 = sm.prs().get(2).unwrap();
    assert_eq!(voter_2.pending_snapshot, 0);
    assert_eq!(voter_2.next_idx, 12);
    assert!(voter_2.paused);
}

#[test]
fn test_snapshot_abort() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    // A successful MsgAppendResponse that has a higher/equal index than the
    // pending snapshot should abort the pending snapshot.
    sm.step(m).expect("");
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
}

// Initialized storage should be at term 1 instead of 0. Otherwise the case will fail.
#[test]
fn test_snapshot_with_min_term() {
    let l = default_logger();
    let do_test = |pre_vote: bool| {
        let s = new_storage();
        s.wl()
            .apply_snapshot(new_snapshot(1, 1, vec![1, 2]))
            .unwrap();
        let n1 = new_test_raft_with_prevote(1, vec![1, 2], 10, 1, s, pre_vote, &l);
        let n2 = new_test_raft_with_prevote(2, vec![], 10, 1, new_storage(), pre_vote, &l);
        let mut nt = Network::new(vec![Some(n1), Some(n2)], &l);
        nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        // 1 will be elected as leader, and then send a snapshot and an empty entry to 2.
        assert_eq!(nt.peers[&2].raft_log.first_index(), 2);
        assert_eq!(nt.peers[&2].raft_log.last_index(), 2);
    };
    do_test(true);
    do_test(false);
}

#[test]
fn test_request_snapshot() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    // Raft can not step request snapshot if there is no leader.
    assert_eq!(
        sm.raft.as_mut().unwrap().request_snapshot().unwrap_err(),
        Error::RequestSnapshotDropped
    );

    let term = sm.term;
    sm.become_follower(term + 1, 2);

    // Raft can not step request snapshot if last raft log's term mismatch current term.
    assert_eq!(
        sm.raft.as_mut().unwrap().request_snapshot().unwrap_err(),
        Error::RequestSnapshotDropped
    );

    sm.become_candidate();
    sm.become_leader();

    // Raft can not step request snapshot if itself is a leader.
    assert_eq!(
        sm.raft.as_mut().unwrap().request_snapshot().unwrap_err(),
        Error::RequestSnapshotDropped
    );

    // Advance matched.
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Replicate);

    let request_snapshot_idx = sm.raft_log.committed;
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    m.reject = true;
    m.reject_hint = INVALID_INDEX;
    m.request_snapshot = request_snapshot_idx;

    // Ignore out of order request snapshot messages.
    let mut out_of_order = m.clone();
    out_of_order.index = 9;
    sm.step(out_of_order).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Replicate);

    // Request snapshot.
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Snapshot);
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 11);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
    assert!(sm.prs().get(2).unwrap().is_paused());
    let snap = sm.msgs.pop().unwrap();
    assert!(
        snap.get_msg_type() == MessageType::MsgSnapshot
            && snap.get_snapshot().get_metadata().index == request_snapshot_idx,
        "{:?}",
        snap
    );

    // Append/heartbeats does not set the state from snapshot to probe.
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Snapshot);
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 11);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
    assert!(sm.prs().get(2).unwrap().is_paused());

    // However snapshot status report does set the stat to probe.
    let m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Probe);
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
    assert!(sm.prs().get(2).unwrap().is_paused());
}
