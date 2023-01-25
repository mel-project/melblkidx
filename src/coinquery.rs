use std::{ops::RangeBounds, sync::Arc};

use genawaiter::sync::Gen;
use itertools::Itertools;
use rusqlite::ToSql;
use themelio_structs::{Address, BlockHeight, CoinData, CoinValue, Denom, TxHash};

use crate::{pool::Pool, BalanceTracker};

/// Info about a coin.
#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct CoinInfo {
    pub create_txhash: TxHash,
    pub create_index: u8,
    pub create_height: BlockHeight,
    pub coin_data: CoinData,
    pub spend_info: Option<CoinSpendInfo>,
}

/// Info about how the coin was spent.
#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Copy)]
pub struct CoinSpendInfo {
    pub spend_txhash: TxHash,
    pub spend_index: usize,
    pub spend_height: BlockHeight,
}

/// A half-built query on the coins table
#[derive(Clone)]
pub struct CoinQuery {
    pub(crate) pool: Pool,

    filters: Vec<String>,
    params: Vec<Arc<dyn ToSql>>,
}

// TODO get rid of this
unsafe impl Send for CoinQuery {}

impl CoinQuery {
    pub(crate) fn new(pool: Pool) -> Self {
        Self {
            pool,
            filters: vec![],
            params: vec![],
        }
    }

    /// Adds a constraint on the creation txhash.
    pub fn create_txhash(self, txhash: TxHash) -> Self {
        self.add_eq_filter("create_txhash", txhash.to_string())
    }

    /// Adds a constraint on the creation index.
    pub fn create_index(self, create_index: u8) -> Self {
        self.add_eq_filter("create_index", create_index)
    }

    /// Adds a constraint on the creation height.
    pub fn create_height_range(self, range: impl RangeBounds<u64>) -> Self {
        self.add_range_filter("create_height", range, |f| *f)
    }

    /// Adds a constraint that filters only for unspent coins.
    pub fn unspent(mut self) -> Self {
        self.filters.push("spend_txhash is null".into());
        self
    }

    /// Adds a constraint on the spending txhash.
    pub fn spend_txhash(self, txhash: TxHash) -> Self {
        self.add_eq_filter("spend_txhash", txhash.to_string())
    }

    /// Adds a constraint on the creation index.
    pub fn spend_index(self, spend_index: u8) -> Self {
        self.add_eq_filter("spend_index", spend_index)
    }

    /// Adds a constraint on the creation height.
    pub fn spend_height_range(self, range: impl RangeBounds<u64>) -> Self {
        self.add_range_filter("spend_height", range, |f| *f)
    }

    /// Adds a constraint on the value.
    pub fn value_range(self, range: impl RangeBounds<CoinValue>) -> Self {
        self.add_range_filter("value", range, |f| f.0.to_be_bytes())
    }

    /// Adds a constraint on the denom.
    pub fn denom(self, denom: Denom) -> Self {
        self.add_eq_filter("denom", denom.to_bytes().to_vec())
    }

    /// Adds a constraint on the covhash.
    pub fn covhash(self, covhash: Address) -> Self {
        self.add_eq_filter("covhash", covhash.to_string())
    }

    /// Adds a constraint on the additional data.
    pub fn additional_data(self, additional_data: &[u8]) -> Self {
        self.add_eq_filter("additional_data", additional_data.to_vec())
    }

    fn add_eq_filter<T: ToSql + 'static>(mut self, field: &str, val: T) -> Self {
        self.filters.push(format!("{} == ?", field));
        self.params.push(Arc::new(val));
        self
    }

    fn add_range_filter<T, U: ToSql + 'static>(
        mut self,
        field: &str,
        range: impl RangeBounds<T>,
        f: impl Fn(&T) -> U,
    ) -> Self {
        match range.start_bound() {
            std::ops::Bound::Included(v) => {
                self.filters.push(format!("{} >= ?", field));
                self.params.push(Arc::new(f(v)));
            }
            std::ops::Bound::Excluded(v) => {
                self.filters.push(format!("{} > ?", field));
                self.params.push(Arc::new(f(v)));
            }
            std::ops::Bound::Unbounded => {}
        }

        match range.end_bound() {
            std::ops::Bound::Included(v) => {
                self.filters.push(format!("{} <= ?", field));
                self.params.push(Arc::new(f(v)));
            }
            std::ops::Bound::Excluded(v) => {
                self.filters.push(format!("{} < ?", field));
                self.params.push(Arc::new(f(v)));
            }
            std::ops::Bound::Unbounded => {}
        }
        self
    }

    /// Create a cached balance tracker from this query.
    pub fn balance_tracker(self) -> BalanceTracker {
        BalanceTracker::new(self)
    }

    /// Iterate through all the coins matching this filter
    pub fn iter(&self) -> impl Iterator<Item = CoinInfo> + '_ {
        let gen = Gen::new(|co| async move {
            let query = format!(
                "select * from coins where {}",
                self.filters.iter().join(" and ")
            );
            log::debug!("iter query: {:?}", query);
            let conn = self.pool.get_conn();
            let mut stmt = conn.prepare_cached(&query).unwrap();
            let params: Vec<&dyn ToSql> = self.params.iter().map(|f| f.as_ref()).collect_vec();
            let i = stmt
                .query_map(&params[..], |row| {
                    let create_txhash: String = row.get(0)?;
                    let create_txhash = TxHash(create_txhash.parse().unwrap());
                    let create_index: u8 = row.get(1)?;
                    let create_height: BlockHeight = BlockHeight(row.get(2)?);
                    let spend_txhash: Option<String> = row.get(3)?;
                    let spend_txhash: Option<TxHash> =
                        spend_txhash.map(|x| TxHash(x.parse().unwrap()));
                    let spend_index: Option<usize> = row.get(4)?;
                    let spend_height: Option<u64> = row.get(5)?;
                    let spend_height: Option<BlockHeight> = spend_height.map(|h| h.into());
                    let value: CoinValue = u128::from_be_bytes(row.get(6)?).into();
                    let denom: Vec<u8> = row.get(7)?;
                    let denom: Denom = Denom::from_bytes(&denom).unwrap();
                    let covhash: String = row.get(8)?;
                    let covhash: Address = covhash.parse().unwrap();
                    let additional_data: Vec<u8> = row.get(9)?;
                    Ok(CoinInfo {
                        create_txhash,
                        create_index,
                        create_height,
                        coin_data: CoinData {
                            covhash,
                            value,
                            denom,
                            additional_data: additional_data.into(),
                        },
                        spend_info: spend_txhash.map(|spend_txhash| CoinSpendInfo {
                            spend_txhash,
                            spend_index: spend_index.unwrap(),
                            spend_height: spend_height.unwrap(),
                        }),
                    })
                })
                .unwrap();
            for elem in i {
                co.yield_(elem.unwrap()).await;
            }
        });
        gen.into_iter()
    }
}
