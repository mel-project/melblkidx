use std::collections::BTreeMap;

use parking_lot::Mutex;
use themelio_structs::CoinValue;

use crate::CoinQuery;

/// Tracks the balance (sum of values) of all coins fulfilling some condition specified by the given CoinQuery, that are alive at a given height. Intelligently caches and plans around previous queries to avoid scanning all coins.
pub struct BalanceTracker {
    query: CoinQuery,
    cache: Mutex<BTreeMap<u64, CoinValue>>,
}

unsafe impl Sync for BalanceTracker {}

impl BalanceTracker {
    /// Returns how much the balance changed between the start (not inclusive) and the end (inclusive)
    fn balance_diff(&self, start: u64, end: u64) -> i128 {
        // Coins created between the two, minus coins spent
        self.query
            .clone()
            .create_height_range(start + 1..=end)
            .iter()
            .fold(0i128, |a, b| a + b.coin_data.value.0 as i128)
            - self
                .query
                .clone()
                .spend_height_range(start + 1..=end)
                .iter()
                .fold(0i128, |a, b| a + b.coin_data.value.0 as i128)
    }

    /// Creates a new balance tracker
    pub fn new(query: CoinQuery) -> Self {
        Self {
            query,
            cache: Default::default(),
        }
    }

    /// Queries the balance at a given height.
    pub fn balance_at(&self, height: u64) -> Option<CoinValue> {
        if let Some(val) = self.cache.lock().get(&height).copied() {
            log::debug!("{} direct hit", height);
            return Some(val);
        }
        // If the cache is empty, just go from scratch
        if self.cache.lock().is_empty() {
            let result = self
                .query
                .clone()
                .create_height_range(..=height)
                .unspent()
                .iter()
                .map(|s| s.coin_data.value)
                .fold(CoinValue(0), |a, b| a + b)
                + self
                    .query
                    .clone()
                    .create_height_range(..=height)
                    .spend_height_range(height + 1..)
                    .iter()
                    .map(|s| s.coin_data.value)
                    .fold(CoinValue(0), |a, b| a + b);
            self.cache.lock().insert(height, result);
            log::debug!("{} total miss", height);
            return Some(result);
        }
        // Find the closest height in the cache
        let (next_height, next_balance) = {
            self.cache
                .lock()
                .range(height + 1..)
                .next()
                .map(|(a, b)| (*a, *b))
                .unwrap_or((u64::MAX, CoinValue(0)))
        };
        let (prev_height, prev_balance) = {
            self.cache
                .lock()
                .range(..height)
                .next_back()
                .map(|(a, b)| (*a, *b))
                .unwrap_or((u64::MAX, CoinValue(0)))
        };
        let balance = if height.abs_diff(next_height) < height.abs_diff(prev_height) {
            log::debug!("{} from next {}", height, next_height);
            // compute from *next* closest height
            let diff = self.balance_diff(height, next_height);
            next_balance - (diff as u128).into()
        } else {
            log::debug!("{} from prev {}", height, prev_height);
            // compute from *previous * closest height
            let diff = self.balance_diff(prev_height, height);
            prev_balance + (diff as u128).into()
        };
        if balance != prev_balance && balance != next_balance {
            self.cache.lock().insert(height, balance);
            // only insert when we have "complete" info here
        }
        Some(balance)
    }
}
