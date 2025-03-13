//! Handle adding history nodes for the latest network upgrade to the database.

use std::collections::BTreeMap;

use crossbeam_channel::{Receiver, TryRecvError};

use zebra_chain::{
    block::Height,
    history_tree::{HistoryTree, NonEmptyHistoryTree},
    parameters::NetworkUpgrade,
    primitives::zcash_history::HistoryNodeIndex,
};

use crate::{service::finalized_state::ZebraDb, HashOrHeight};

use super::{super::super::DiskWriteBatch, CancelFormatChange};

/// Returns `Ok` if the upgrade completed, and `Err` if it was cancelled.
#[allow(clippy::unwrap_in_result)]
#[instrument(skip(zebra_db, cancel_receiver))]
pub fn run(
    initial_tip_height: Height,
    zebra_db: &ZebraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    let history_tree = zebra_db.history_tree();

    if history_tree.is_some() {
        let network = <std::option::Option<NonEmptyHistoryTree> as Clone>::clone(&history_tree)
            .unwrap()
            .network()
            .clone();
        let activation_height = NetworkUpgrade::current(&network, initial_tip_height)
            .activation_height(&network)
            .expect("activation height should be valid");

        // Read all blocks between the activation height and the current height from db, push them into a new
        // history tree, and store the returned nodes. Finally, write the nodes to the database.
        let mut tree = HistoryTree::default();
        let mut batch = DiskWriteBatch::new();

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        for h in activation_height.as_usize()..=initial_tip_height.as_usize() {
            let block = zebra_db
                .block(HashOrHeight::Height(
                    Height::try_from(h as u32).expect("the value was already a valid height"),
                ))
                .unwrap();
            let sapling_root = zebra_db
                .sapling_tree_by_height(
                    &Height::try_from(h as u32).expect("the value was already a valid height"),
                )
                .unwrap()
                .root();
            let orchard_root = zebra_db
                .orchard_tree_by_height(
                    &Height::try_from(h as u32).expect("the value was already a valid height"),
                )
                .unwrap()
                .root();
            let nodes = tree
                .push(&zebra_db.network(), block, &sapling_root, &orchard_root)
                .expect("pushing already finalized block into new history tree should succeed");

            batch.append_history_nodes(&zebra_db, nodes);

            if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                return Err(CancelFormatChange);
            }
        }

        zebra_db
            .write_batch(batch)
            .expect("unexpected database write failure");

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }
    }

    // If the history tree is empty, there is nothing to do

    Ok(())
}

/// Checks if the upgrade was successful.
pub fn check(
    db: &ZebraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<Result<(), String>, CancelFormatChange> {
    // Get the existing tip history tree
    let history_tree = db.history_tree();

    if history_tree.is_some() {
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        // Calculate the peaks
        let network = <std::option::Option<NonEmptyHistoryTree> as Clone>::clone(&history_tree)
            .unwrap()
            .network()
            .clone();
        let height = <std::option::Option<NonEmptyHistoryTree> as Clone>::clone(&history_tree)
            .unwrap()
            .current_height();
        let size = <std::option::Option<NonEmptyHistoryTree> as Clone>::clone(&history_tree)
            .unwrap()
            .size();

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        let peaks_idx = history_tree
            .peaks_at(height)
            .expect("current height should be valid");
        let peaks = peaks_idx
            .into_iter()
            .map(|idx| {
                (
                    idx,
                    db.history_node(HistoryNodeIndex {
                        upgrade: NetworkUpgrade::current(&network, height),
                        index: idx,
                    })
                    .unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        // Build a new history tree from the nodes
        let new_tree = HistoryTree::from(
            NonEmptyHistoryTree::from_cache(&network, size, peaks, height)
                .expect("history tree should be valid"),
        );

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        // Compare the hashes
        let old_hash = history_tree.hash().expect("tree is not empty");
        let new_hash = new_tree.hash().expect("tree is not empty");
        if old_hash != new_hash {
            return Ok(Err("history tree hash mismatch".to_string()));
        } else {
            return Ok(Ok(()));
        }
    }

    // Nothing to do if the history tree is empty
    Ok(Ok(()))
}
