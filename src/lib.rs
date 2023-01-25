#![doc = include_str!("../README.md")]

mod balance;
mod coinquery;
pub use balance::*;
pub use coinquery::*;
use tap::Tap;
use tmelcrypt::HashVal;
mod pool;

use std::{collections::HashMap, path::Path, time::Duration};

use itertools::Itertools;
use pool::Pool;
use rusqlite::{params, OptionalExtension};
use smol::Task;
use themelio_nodeprot::ValClient;
use themelio_structs::{BlockHeight, CoinID, StakeDoc, TxHash, TxKind};

/// An asynchronous Melodeon block indexer.
pub struct Indexer {
    /// At the moment, just a single connection, letting us stop worrying about retrying txx etc
    pool: Pool,

    _task: Task<()>,
}

impl Indexer {
    /// Creates a new indexer based on the given path to an SQLite database and ValClient.
    pub fn new(path: impl AsRef<Path>, client: ValClient) -> rusqlite::Result<Self> {
        let pool = Pool::open(path)?;
        let db = pool.get_conn();
        db.execute(r"create table if not exists coins (create_txhash not null, create_index not null, create_height not null, spend_txhash, spend_index, spend_height, value not null, denom not null, covhash not null, additional_data not null,
            UNIQUE(create_txhash, create_index, create_height) ON CONFLICT IGNORE
        )
        ", [])?;
        db.execute(
            r"create index if not exists coins_owner on coins(covhash)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_balance on coins(covhash, spend_txhash)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_balance1 on coins(covhash, spend_height)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_supply on coins(create_height, spend_height)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_supply1 on coins(create_height, spend_txhash)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_denom on coins(denom)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_spender on coins(spend_txhash)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_spender on coins(spend_txhash)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_createheight on coins(create_height)",
            [],
        )?;
        db.execute(
            r"create index if not exists coins_spendheight on coins(spend_height)",
            [],
        )?;
        db.execute(r"create table if not exists headvars (height primary key not null, blkhash not null, fee_pool not null, fee_multiplier not null, dosc_speed not null, UNIQUE(height) ON CONFLICT IGNORE
        )
        ", [])?;
        db.execute(r"create table if not exists stakes (txhash primary key not null, pubkey not null, e_start not null, e_post_end not null, staked not null, UNIQUE(txhash) ON CONFLICT IGNORE
        )
        ", [])?;
        db.execute(r"create table if not exists txvars (txhash primary key not null, kind not null, fee not null, covenants not null, data not null, sigs not null, UNIQUE(txhash) ON CONFLICT IGNORE
        )
        ", [])?;
        log::debug!("spawning indexer loop");
        let _task = smolscale::spawn(indexer_loop(pool.clone(), client));
        Ok(Self { pool, _task })
    }

    /// Creates an object for querying the coins
    pub fn query_coins(&self) -> CoinQuery {
        CoinQuery::new(self.pool.clone())
    }

    /// Get miscellaneous info about a height
    pub fn height_info(&self, height: BlockHeight) -> Option<HeightInfo> {
        let conn = self.pool.get_conn();
        conn.query_row(
            "select * from headvars where height = $1",
            params![height.0],
            |row| {
                let height = BlockHeight(row.get(0)?);
                let blkhash: String = row.get(1)?;
                let blkhash: HashVal = blkhash.parse().unwrap();
                let fee_pool = u128::from_be_bytes(row.get(2)?);
                let fee_multiplier = u128::from_be_bytes(row.get(3)?);
                let dosc_speed = u128::from_be_bytes(row.get(4)?);
                Ok(HeightInfo {
                    height,
                    blkhash,
                    fee_pool,
                    fee_multiplier,
                    dosc_speed,
                })
            },
        )
        .optional()
        .unwrap()
    }

    /// Get the max height
    pub fn max_height(&self) -> BlockHeight {
        let conn = self.pool.get_conn();
        conn.query_row("select max(height) from headvars", params![], |r| {
            Ok(BlockHeight(r.get(0)?))
        })
        .unwrap()
    }

    /// Search for a transaction by hash. Returns the block in which it can be found.
    pub fn txhash_to_height(&self, txhash: TxHash) -> Option<BlockHeight> {
        // TODO a better strategy?
        self.query_coins()
            .spend_txhash(txhash)
            .iter()
            .map(|c| c.spend_info.unwrap().spend_height)
            .next()
    }

    /// Search for a block by hash.
    pub fn blkhash_to_height(&self, blkhash: HashVal) -> Option<BlockHeight> {
        let conn = self.pool.get_conn();
        conn.query_row(
            "select height from headvars where blkhash = $1",
            params![blkhash.to_string()],
            |row| Ok(BlockHeight(row.get(0)?)),
        )
        .optional()
        .unwrap()
    }
}

/// Miscellenous info about height
pub struct HeightInfo {
    pub height: BlockHeight,
    pub blkhash: HashVal,
    pub fee_pool: u128,
    pub fee_multiplier: u128,
    pub dosc_speed: u128,
}

async fn indexer_loop(pool: Pool, client: ValClient) {
    loop {
        if let Err(err) = indexer_loop_once(pool.clone(), client.clone()).await {
            log::warn!("indexing failed with {:?}, restarting", err)
        }
        smol::Timer::after(Duration::from_secs(1)).await;
    }
}

async fn indexer_loop_once(pool: Pool, client: ValClient) -> anyhow::Result<()> {
    // first, we find out the highest height we have
    let our_highest: u64 =
        pool.get_conn()
            .query_row("select coalesce(max(height),0) from headvars", [], |d| {
                d.get(0)
            })?;
    // then find their highest
    let highest_snap = client.snapshot().await?;
    let their_highest = highest_snap.current_header().height;
    let mut last_stakes = None;
    for height in ((our_highest + 1)..=their_highest.0).map(BlockHeight) {
        // // "preload" a bunch, horrible hack
        // {
        //     let vv = (0..256)
        //         .map(|i| {
        //             let highest_snap = highest_snap.clone();
        //             smolscale::spawn(async move {
        //                 highest_snap.get_older(height + BlockHeight(i)).await
        //             })
        //         })
        //         .collect_vec();
        //     for v in vv {
        //         let _ = v.await;
        //     }
        // }

        let snap = highest_snap.get_older(height).await?;
        let blk = snap.current_block().await?;
        // get all the coins produced
        let mut new_coins = HashMap::new();
        let mut spent_coins = HashMap::new();
        if let Some(cdh) = snap.get_coin(CoinID::proposer_reward(height)).await? {
            new_coins.insert(CoinID::proposer_reward(height), cdh.coin_data);
        }
        for tx in blk.transactions.iter() {
            for (i, output) in tx.outputs.iter().enumerate() {
                new_coins.insert(CoinID::new(tx.hash_nosigs(), i as _), output.clone());
            }
            if tx.kind == TxKind::Swap {
                let id = CoinID::new(tx.hash_nosigs(), 0);
                if let Some(coin) = snap.get_coin(id).await? {
                    new_coins.insert(id, coin.coin_data);
                }
            }
            for (i, input) in tx.inputs.iter().enumerate() {
                spent_coins.insert(*input, (tx.hash_nosigs(), i));
            }
        }
        // update stake mapping
        let stakes = if last_stakes != Some(blk.header.stakes_hash) {
            last_stakes = Some(blk.header.stakes_hash);
            // TODO: validate?
            snap.get_raw().get_stakers_raw(height).await?
        } else {
            None
        };
        log::trace!("indexed {}", height);
        // commit the stuff into the database
        let mut conn = pool.get_conn();
        let conn = conn.transaction()?;
        for (new_coin, new_coindata) in new_coins {
            conn.execute(
                "insert into coins values ($1, $2, $3, NULL, NULL, NULL, $4, $5, $6, $7)",
                params![
                    new_coin.txhash.to_string(),
                    new_coin.index,
                    height.0,
                    new_coindata.value.0.to_be_bytes().to_vec(),
                    new_coindata.denom.to_bytes().to_vec(),
                    new_coindata.covhash.to_string(),
                    new_coindata.additional_data.to_vec()
                ],
            )?;
        }
        for (spent_coin, (spend_txhash, spend_idx)) in spent_coins {
            conn.execute(
                "update coins set spend_txhash = $1, spend_index = $2, spend_height = $3 where create_txhash = $4 and create_index = $5",
                params![
                    spend_txhash.to_string(),
                    spend_idx,
                    height.0,
                    spent_coin.txhash.to_string(),
                    spent_coin.index
                ],
            )?;
        }
        // update header variables
        conn.execute(
            "insert into headvars values ($1, $2, $3, $4, $5)",
            params![
                height.0,
                blk.header.hash().to_string(),
                blk.header.fee_pool.0.to_be_bytes(),
                blk.header.fee_multiplier.to_be_bytes(),
                blk.header.dosc_speed.to_be_bytes()
            ],
        )?;
        // update stakers
        if let Some(stakes) = stakes {
            for (txhash, doc) in stakes {
                let doc: StakeDoc = stdcode::deserialize(&doc).unwrap();
                conn.execute(
                    "insert into stakes values ($1, $2, $3, $4, $5)",
                    params![
                        txhash.to_string(),
                        doc.pubkey.0.to_vec(),
                        doc.e_start,
                        doc.e_post_end,
                        doc.syms_staked.0.to_be_bytes()
                    ],
                )?;
            }
        }
        // update transactions
        for txn in blk.transactions.iter() {
            conn.execute(
                "insert into txvars values ($1, $2, $3, $4, $5, $6)",
                params![
                    txn.hash_nosigs().to_string(),
                    u8::from(txn.kind),
                    txn.fee.0.to_be_bytes(),
                    serde_json::to_string(&txn.covenants.iter().map(hex::encode).collect_vec())
                        .unwrap(),
                    txn.data.clone().tap_mut(|d| { d.truncate(1024) }).to_vec(), // only keep first kilobyte
                    serde_json::to_string(&txn.sigs.iter().map(hex::encode).collect_vec()).unwrap()
                ],
            )?;
        }
        conn.commit()?;
        log::trace!("committed {}", height);
    }
    Ok(())
}
