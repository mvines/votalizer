use {
    crate::{notifier::*, tower::*},
    clap::{crate_description, crate_name, crate_version, App, Arg},
    futures_util::StreamExt,
    itertools::Itertools,
    log::*,
    solana_clap_utils::input_validators::{is_url_or_moniker, normalize_to_url_if_moniker},
    solana_client::nonblocking::pubsub_client::PubsubClient,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        fs::File,
        time::{Duration, Instant},
    },
};

mod notifier;
mod tower;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(crate_version!())
        .arg(
            Arg::with_name("json_rpc_url")
                .short("u")
                .long("url")
                .value_name("URL")
                .takes_value(true)
                .validator(is_url_or_moniker)
                .default_value("localhost")
                .help("JSON RPC URL for the cluster"),
        )
        .get_matches();

    let json_rpc_url = normalize_to_url_if_moniker(matches.value_of("json_rpc_url").unwrap());
    let websocket_url = solana_cli_config::Config::compute_websocket_url(&json_rpc_url);

    let notifier = Notifier::default();
    solana_logger::setup_with_default("info");

    info!("websocket URL: {}", websocket_url);

    let pubsub_client = PubsubClient::new(&websocket_url).await?;
    let (mut votes, votes_unsubscribe) = pubsub_client.vote_subscribe().await?;
    let (mut slots, slots_unsubscribe) = pubsub_client.slot_subscribe().await?;

    let mut slot_ancestors = BTreeMap::<Slot, HashSet<Slot>>::new();
    let mut towers = HashMap::<Pubkey, Tower>::new();
    let mut processed_vote_counter = 0;
    let mut incident_counter = 0;
    let mut last_status_report = Instant::now();
    let mut last_notifier_status_report = Instant::now();

    const MAX_TRACKED_ANCESTORS: usize = 10 * 1_024;
    const MAX_TRACKED_SLOTS: usize = 10 * 1_024;

    let _ = notifier.send("votalizer active").await;
    loop {
        tokio::select! {
            Some(slot_info) = slots.next() => {
                if slot_ancestors.contains_key(&slot_info.slot) {
                    warn!("slot {} already present in slot_ancestors. RPC node stuck?", slot_info.slot);
                } else {
                    let parent_ancestors = slot_ancestors.entry(slot_info.parent).or_default();

                    let mut ancestors = parent_ancestors.clone();
                    ancestors.insert(slot_info.parent);
                    while ancestors.len() > MAX_TRACKED_ANCESTORS {
                        let min = *ancestors.iter().min().unwrap();
                        ancestors.remove(&min);
                    }

                    info!(
                        "slot: {} (parent: {}, {} tracked ancestors)",
                        slot_info.slot,
                        slot_info.parent,
                        ancestors.len()
                    );
                    slot_ancestors.insert(slot_info.slot, ancestors);

                    while slot_ancestors.len() > MAX_TRACKED_SLOTS {
                        let slot_to_remove = *slot_ancestors.keys().next().unwrap();
                        slot_ancestors.remove(&slot_to_remove);
                    }

                    let now = Instant::now();
                    if now.duration_since(last_status_report) > Duration::from_secs(30) {
                        let status_report = format!(
                            "tracking {} validators, {} votes processed{}",
                            towers.len(),
                            processed_vote_counter,
                            if incident_counter > 1 {
                                format!(", {} incidents observed", incident_counter)
                            } else if incident_counter > 0 {
                                ", 1 incident observed".into()
                            } else {
                                "".into()
                            }
                        );

                        info!("{}", status_report);
                        if now.duration_since(last_notifier_status_report) > Duration::from_secs(60 * 60 * 12) {
                            notifier.send(&status_report).await;
                            last_notifier_status_report = now;
                        }

                        last_status_report = now;
                    }
                }
            },
            Some(mut vote) = votes.next() => {
                let vote_account_address = vote.vote_pubkey.parse::<Pubkey>().unwrap();
                let signature = vote.signature.parse::<Signature>().unwrap();

                if vote.timestamp.is_none() {
                    // TODO: if `timestamp.is_some()`, consider looking for unusual values
                    debug!("{} did not publish a timestamp", vote.vote_pubkey);
                }

                let tower = towers.entry(vote_account_address).or_default();

                vote.slots.sort_unstable();
                vote.slots.dedup();
                for pair in vote.slots.chunks_exact(2) {
                    if pair[0] >= pair[1] {
                        panic!(
                            "{}: Invalid vote pair, {:?}, in {}",
                            vote_account_address, pair, signature
                        );
                    }
                }

                // Ignore votes for slots earlier than we already have votes for
                let new_votes = vote
                    .slots
                    .iter()
                    .cloned()
                    .filter(|slot| {
                        tower
                            .last_voted_slot()
                            .map_or(true, |last_voted_slot| *slot > last_voted_slot)
                    })
                    .collect::<Vec<_>>();

                if !new_votes.is_empty() {
                    trace!(
                        "{:<44}: new votes: {} [{}]",
                        vote_account_address,
                        new_votes.iter().map(ToString::to_string).join(", "),
                        signature
                    );

                    tower.record_vote_signature(signature, new_votes.clone());

                    for slot in new_votes {
                        processed_vote_counter += 1;

                        if let Some(incident) = tower.process_vote_slot(
                            &vote_account_address,
                            slot,
                            &signature,
                            &slot_ancestors,
                        ) {
                            let msg = format!(
                                "{}: Lockout violation detected [{}]",
                                vote_account_address, signature
                            );
                            notifier.send(&msg).await;
                            error!("{}\n{}", msg, incident);
                            let filename =
                                format!("incident-{}-{}.log", vote_account_address, signature);

                            File::create(&filename)
                                .and_then(|mut output| {
                                    use std::io::Write;
                                    writeln!(output, "{}", incident)
                                })
                                .unwrap_or_else(|err| error!("Unable to write {}: {}", filename, err));
                            incident_counter += 1;
                        }
                    }
                }
            },
            else => {
                break;
            }
        }
    }
    slots_unsubscribe().await;
    votes_unsubscribe().await;

    Ok(())
}
