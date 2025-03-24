//! Handle adding history nodes for the latest network upgrade to the database.

use std::collections::BTreeMap;

use crossbeam_channel::{Receiver, TryRecvError};

use zebra_chain::{
    block::Height,
    history_tree::{HistoryTree, NonEmptyHistoryTree},
    parameters::{Network, NetworkUpgrade},
    primitives::zcash_history::HistoryNodeIndex,
};

use crate::{service::finalized_state::ZebraDb, HashOrHeight};

use super::{super::super::DiskWriteBatch, CancelFormatChange};

fn upgrades_with_history(network: Network) -> [(NetworkUpgrade, Option<Height>); 4] {
    [
        (
            NetworkUpgrade::Heartwood,
            NetworkUpgrade::activation_height(&NetworkUpgrade::Heartwood, &network),
        ),
        (
            NetworkUpgrade::Canopy,
            NetworkUpgrade::activation_height(&NetworkUpgrade::Canopy, &network),
        ),
        (
            NetworkUpgrade::Nu5,
            NetworkUpgrade::activation_height(&NetworkUpgrade::Nu5, &network),
        ),
        (
            NetworkUpgrade::Nu6,
            NetworkUpgrade::activation_height(&NetworkUpgrade::Nu6, &network),
        ),
    ]
}

/// Returns `Ok` if the upgrade completed, and `Err` if it was cancelled.
#[allow(clippy::unwrap_in_result)]
#[instrument(skip(zebra_db, cancel_receiver))]
pub fn run(
    initial_tip_height: Height,
    zebra_db: &ZebraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    info!("Running history node db upgrade");

    // Clear current data if it exists
    let mut batch_for_delete = DiskWriteBatch::new();
    batch_for_delete.clear_history_nodes(zebra_db);
    zebra_db
        .write_batch(batch_for_delete)
        .expect("unexpected database write failure");

    let history_tree = zebra_db.history_tree();

    if history_tree.is_none() {
        // If the history tree is empty, there is nothing to do
        return Ok(());
    }

    let network = zebra_db.network().clone();
    let upgrades_with_history = upgrades_with_history(network.clone());

    // Iterate over all network upgrades with history nodes
    for (upgrade, activation_height_option) in upgrades_with_history.iter() {
        info!("Fetching history nodes for {}", upgrade);
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        // We reached a future upgrade, or the upgrade is not available
        if activation_height_option.is_none() {
            break;
        }

        let activation_height = activation_height_option.unwrap();

        // We reached the tip
        if initial_tip_height < activation_height {
            break;
        }

        let mut inner_tree: Option<NonEmptyHistoryTree> = None;

        // Read all blocks before the tip for this network upgrade from db, push them into a new
        // history tree, and store the returned nodes. Finally, write the nodes to the database.
        let last_block_height = upgrade
            .next_upgrade()
            .and_then(|u| u.activation_height(&network).map(|h| h - 1))
            .flatten()
            .unwrap_or(initial_tip_height)
            .min(initial_tip_height);

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        let mut batch = DiskWriteBatch::new();
        let mut index = 0u32;
        for h in activation_height.as_usize()..=last_block_height.as_usize() {
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

            let nodes = match inner_tree {
                Some(ref mut t) => t
                    .push(block, &sapling_root, &orchard_root)
                    .expect("pushing already finalized block into new history tree should succeed"),
                None => {
                    inner_tree = Some(
                        NonEmptyHistoryTree::from_block(
                            &network,
                            block,
                            &sapling_root,
                            &orchard_root,
                        )
                        .expect("history tree should exist post-Heartwood"),
                    );
                    let t = Option::<NonEmptyHistoryTree>::clone(&inner_tree).unwrap();
                    t.peaks().iter().map(|n| n.1.clone()).collect::<Vec<_>>()
                }
            };

            // info!("Adding {} history nodes for block {}", nodes.iter().count(), h);

            // batch.append_history_nodes(zebra_db, nodes, *upgrade);

            nodes.iter().for_each(|node| {
                // info!("Writing {:?} history node {}", upgrade, index);
                batch.write_history_node(
                    zebra_db,
                    HistoryNodeIndex {
                        upgrade: *upgrade,
                        index,
                    },
                    node.clone(),
                );
                index += 1;
            });

            if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                return Err(CancelFormatChange);
            }
        }

        info!("Adding {} history nodes for upgrade {:?}", index, upgrade);
        zebra_db
            .write_batch(batch)
            .expect("unexpected database write failure");
    }

    Ok(())
}

/// Checks if the upgrade was successful.
pub fn check(
    initial_tip_height: Height,
    zebra_db: &ZebraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<Result<(), String>, CancelFormatChange> {
    info!("Checking history node db upgrade");

    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(CancelFormatChange);
    }

    let network = zebra_db.network().clone();
    let upgrades_with_history = upgrades_with_history(network.clone());

    // Iterate over all network upgrades with history nodes
    for (upgrade, activation_height_option) in upgrades_with_history.iter() {
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        // We reached a future upgrade, or the upgrade is not available
        if activation_height_option.is_none() {
            break;
        }

        let activation_height = activation_height_option.unwrap();
        let height = initial_tip_height;

        // We reached the tip
        if height < activation_height {
            break;
        }

        let mut inner_tree: Option<NonEmptyHistoryTree> = None;

        let last_block_height = upgrade
            .next_upgrade()
            .and_then(|u| u.activation_height(&network).map(|h| h - 1))
            .flatten()
            .unwrap_or(height)
            .min(height);

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        for h in activation_height.as_usize()..=last_block_height.as_usize() {
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
            match inner_tree {
                Some(ref mut t) => {
                    t.push(block, &sapling_root, &orchard_root).expect(
                        "pushing already finalized block into new history tree should succeed",
                    );
                }
                None => {
                    inner_tree = Some(
                        NonEmptyHistoryTree::from_block(
                            &network,
                            block,
                            &sapling_root,
                            &orchard_root,
                        )
                        .expect("history tree should exist post-Heartwood"),
                    );
                }
            };

            if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                return Err(CancelFormatChange);
            }
        }

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        let history_tree = HistoryTree::from(inner_tree);
        let peaks_idx = history_tree
            .peaks_at(last_block_height)
            .expect("current height should be valid");

        info!(
            "Reading peaks at block {:?}: {:?}",
            last_block_height, peaks_idx
        );

        let peaks = peaks_idx
            .iter()
            .map(|idx| {
                (
                    *idx,
                    zebra_db
                        .history_node(HistoryNodeIndex {
                            upgrade: *upgrade,
                            index: *idx,
                        })
                        .expect("history node should exist"),
                )
            })
            .collect::<BTreeMap<_, _>>();

        // Build a new history tree from the nodes
        let size = <Option<NonEmptyHistoryTree> as Clone>::clone(&history_tree)
            .unwrap()
            .size();
        let new_tree = HistoryTree::from(
            NonEmptyHistoryTree::from_cache(&network, size, peaks, last_block_height)
                .expect("history tree should be valid"),
        );

        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        // Compare the hashes
        let old_hash = history_tree.hash().expect("tree is not empty");
        let new_hash = new_tree.hash().expect("tree is not empty");
        info!("Expected tree root hash: {:x?}", old_hash);
        info!("Calculated tree root hash: {:x?}", new_hash);
        if old_hash != new_hash {
            return Ok(Err(
                format!("History tree hash mismatch for {}", upgrade).to_string()
            ));
        }
    }

    Ok(Ok(()))
}
