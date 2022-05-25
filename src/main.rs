use {
    crate::notifier::*,
    clap::{crate_description, crate_name, crate_version, App, Arg},
    futures_util::StreamExt,
    itertools::Itertools,
    log::*,
    solana_clap_utils::input_validators::{is_url_or_moniker, normalize_to_url_if_moniker},
    solana_client::nonblocking::pubsub_client::PubsubClient,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    solana_vote_program::vote_state::{Lockout, MAX_LOCKOUT_HISTORY},
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        fmt::Write,
        fs::File,
        time::{Duration, Instant},
    },
};

mod notifier;

type Incident = String;

struct Tower {
    votes: VecDeque<(Lockout, Signature)>,
    root_slot: Option<Slot>,
    vote_history: VecDeque<(Signature, Vec<Slot>)>,
}

impl Default for Tower {
    fn default() -> Self {
        Self {
            votes: VecDeque::from(vec![
                (Lockout::default(), Signature::default());
                MAX_LOCKOUT_HISTORY
            ]),
            root_slot: None,
            vote_history: VecDeque::default(),
        }
    }
}

impl Tower {
    fn last_lockout(&self) -> Option<&Lockout> {
        self.votes.back().map(|(lockout, _)| lockout)
    }

    fn last_voted_slot(&self) -> Option<Slot> {
        self.last_lockout().map(|v| v.slot)
    }

    // Pop all recent votes that are not locked out at the next vote slot.  This
    // allows validators to switch forks once their votes for another fork have
    // expired. This also allows validators continue voting on recent blocks in
    // the same fork without increasing lockouts.
    fn pop_expired_votes(&mut self, next_vote_slot: Slot) {
        while let Some(vote) = self.last_lockout() {
            if !vote.is_locked_out_at_slot(next_vote_slot) {
                self.votes.pop_back();
            } else {
                break;
            }
        }
    }

    fn double_lockouts(&mut self) {
        let stack_depth = self.votes.len();
        for (i, v) in self.votes.iter_mut().enumerate() {
            // Don't increase the lockout for this vote until we get more confirmations
            // than the max number of confirmations this vote has seen
            if stack_depth > i + v.0.confirmation_count as usize {
                v.0.confirmation_count += 1;
            }
        }
    }

    fn process_vote_slot(
        &mut self,
        vote_account_address: &Pubkey,
        vote_slot: Slot,
        signature: &Signature,
        slot_ancestors: &BTreeMap<Slot, HashSet<Slot>>,
    ) -> Option<Incident> {
        let mut maybe_incident = None;
        self.pop_expired_votes(vote_slot);

        if let Some(root_slot) = self.root_slot {
            if !slot_ancestors.contains_key(&root_slot) {
                debug!(
                    "{}: Unable to perform lockout check for {} due to unknown root slot {}",
                    vote_account_address, vote_slot, root_slot
                );
            } else if let Some(next_vote_ancestors) = slot_ancestors.get(&vote_slot) {
                if let Some(last_lockout) = self.last_lockout() {
                    if !slot_ancestors.contains_key(&last_lockout.slot) {
                        debug!(
                            "{}: Unable to perform lockout check for {}: last lockout slot {} unknown",
                            vote_account_address, vote_slot,
                            last_lockout.slot,
                        );
                    } else if !next_vote_ancestors.contains(&last_lockout.slot) {
                        let mut incident = String::new();
                        let _ = writeln!(incident, "lockout violation: {}", vote_account_address);
                        let _ = writeln!(incident, "signature: {}", signature);
                        let _ = writeln!(incident, "vote slot: {}", vote_slot);
                        let _ = writeln!(incident, "root slot: {}", root_slot);
                        let _ = writeln!(
                            incident,
                            "last lockout slot: {}",
                            last_lockout.last_locked_out_slot()
                        );
                        let _ = writeln!(incident, "tower:");
                        for lockout in &self.votes {
                            let _ = writeln!(
                                incident,
                                "  - {} (conf: {}), last lockout slot: {} [{}]",
                                lockout.0.slot,
                                lockout.0.confirmation_count,
                                lockout.0.last_locked_out_slot(),
                                lockout.1
                            );
                        }

                        let next_vote_ancestors = next_vote_ancestors
                            .iter()
                            .filter(|slot| **slot >= root_slot)
                            .collect::<HashSet<_>>();

                        let lockout_slot_ancestors = slot_ancestors.get(&last_lockout.slot).unwrap(/* TODO: should never fail right?...*/)
                            .iter()
                            .filter(|slot| **slot >= root_slot)
                            .collect::<HashSet<_>>();

                        let common_ancestors = next_vote_ancestors
                            .intersection(&lockout_slot_ancestors)
                            .collect::<HashSet<_>>();

                        let mut next_vote_ancestors =
                            next_vote_ancestors.iter().copied().collect::<Vec<_>>();
                        next_vote_ancestors.sort();
                        next_vote_ancestors.reverse();
                        let _ = writeln!(
                            incident,
                            "fork at vote slot {} to common ancestor:",
                            vote_slot
                        );
                        let _ = writeln!(
                            incident,
                            "  - {}",
                            next_vote_ancestors
                                .iter()
                                .filter(|x| !common_ancestors.contains(x))
                                .map(ToString::to_string)
                                .join(", ")
                        );

                        let mut lockout_slot_ancestors =
                            lockout_slot_ancestors.iter().copied().collect::<Vec<_>>();
                        lockout_slot_ancestors.sort();
                        lockout_slot_ancestors.reverse();
                        let _ = writeln!(
                            incident,
                            "fork at lockout slot {} to common ancestor:",
                            last_lockout.slot
                        );
                        let _ = writeln!(
                            incident,
                            "  - {}",
                            lockout_slot_ancestors
                                .iter()
                                .filter(|x| !common_ancestors.contains(x))
                                .map(ToString::to_string)
                                .join(", ")
                        );

                        let _ = writeln!(incident, "common fork ancestors:");
                        let mut common_ancestors =
                            common_ancestors.iter().copied().collect::<Vec<_>>();
                        common_ancestors.sort();
                        common_ancestors.reverse();
                        let _ = writeln!(
                            incident,
                            "  - {}",
                            common_ancestors.iter().map(ToString::to_string).join(", ")
                        );

                        let _ = writeln!(incident, "vote transaction history:");
                        for (signature, slots) in &self.vote_history {
                            let _ = writeln!(
                                incident,
                                " - {} [{}]",
                                slots.iter().map(ToString::to_string).join(", "),
                                signature
                            );
                        }

                        maybe_incident = Some(incident);
                    }
                }
            } else {
                debug!(
                    "{}: Unable to perform lockout check for {}: slot unknown",
                    vote_account_address, vote_slot
                );
            }
        } else {
            debug!(
                "{}: Unable to perform lockout check for {}: no root slot",
                vote_account_address, vote_slot
            );
        }

        if self.votes.len() == MAX_LOCKOUT_HISTORY {
            let vote = self.votes.pop_front().unwrap();
            self.root_slot = Some(vote.0.slot);
        }
        self.votes.push_back((Lockout::new(vote_slot), *signature));
        self.double_lockouts();

        maybe_incident
    }
}

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

    const MAX_TRACKED_ANCESTORS: usize = 10 * 1_024;
    const MAX_TRACKED_SLOTS: usize = 10 * 1_024;

    let _ = notifier.send("votalizer active").await;
    loop {
        tokio::select! {
            Some(slot_info) = slots.next() => {
                assert!(
                    !slot_ancestors.contains_key(&slot_info.slot),
                    "slot {} already present in slot_ancestors",
                    slot_info.slot
                );
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
                    info!(
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
                    last_status_report = now;
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

                    tower.vote_history.push_back((signature, new_votes.clone()));
                    if let Some((_, first_vote_signature)) = tower.votes.get(0) {
                        let position = tower
                            .vote_history
                            .iter()
                            .position(|(signature, _)| signature == first_vote_signature);
                        for _ in 0..position.unwrap_or_default() {
                            tower.vote_history.pop_front();
                        }
                    }
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
