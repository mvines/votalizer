use {
    itertools::Itertools,
    log::*,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    solana_vote_program::vote_state::{Lockout, MAX_LOCKOUT_HISTORY},
    std::{
        collections::{BTreeMap, HashSet, VecDeque},
        fmt::Write,
    },
};

pub type Incident = String;

pub struct Tower {
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

    pub fn last_voted_slot(&self) -> Option<Slot> {
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

    #[allow(clippy::too_many_arguments)]
    fn write_incident_report(
        &self,
        vote_account_address: &Pubkey,
        vote_slot: Slot,
        signature: &Signature,
        slot_ancestors: &BTreeMap<Slot, HashSet<Slot>>,
        root_slot: Slot,
        last_lockout: &Lockout,
        next_vote_ancestors: &HashSet<Slot>,
    ) -> Incident {
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

        let lockout_slot_ancestors = slot_ancestors
            .get(&last_lockout.slot)
            .unwrap_or_else(|| {
                panic!(
                    "last lockout slot {} not found in slot ancestors",
                    last_lockout.slot
                );
            })
            .iter()
            .filter(|slot| **slot >= root_slot)
            .collect::<HashSet<_>>();

        let common_ancestors = next_vote_ancestors
            .intersection(&lockout_slot_ancestors)
            .collect::<HashSet<_>>();

        let mut next_vote_ancestors = next_vote_ancestors.iter().copied().collect::<Vec<_>>();
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

        let mut lockout_slot_ancestors = lockout_slot_ancestors.iter().copied().collect::<Vec<_>>();
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
        let mut common_ancestors = common_ancestors.iter().copied().collect::<Vec<_>>();
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

        incident
    }

    pub fn record_vote_signature(&mut self, signature: Signature, new_votes: Vec<Slot>) {
        self.vote_history.push_back((signature, new_votes));
        if let Some((_, first_vote_signature)) = self.votes.get(0) {
            let position = self
                .vote_history
                .iter()
                .position(|(signature, _)| signature == first_vote_signature);
            for _ in 0..position.unwrap_or_default() {
                self.vote_history.pop_front();
            }
        }
    }

    pub fn process_vote_slot(
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
                        maybe_incident = Some(self.write_incident_report(
                            vote_account_address,
                            vote_slot,
                            signature,
                            slot_ancestors,
                            root_slot,
                            last_lockout,
                            next_vote_ancestors,
                        ));
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
