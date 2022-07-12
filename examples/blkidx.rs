use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use itertools::Itertools;
use melblkidx::Indexer;
use themelio_nodeprot::ValClient;
use themelio_structs::{Address, CoinValue, Denom, NetID};

fn main() {
    env_logger::init();
    let client = ValClient::new(NetID::Mainnet, "127.0.0.1:11814".parse().unwrap());
    client.trust(themelio_bootstrap::checkpoint_height(NetID::Mainnet).unwrap());
    let indexer = Indexer::new("./test.db", client).unwrap();
    // println!("covhash,value");
    // let mut sum: HashMap<Address, CoinValue> = HashMap::new();
    // for item in indexer
    //     .query_coins()
    //     .denom(Denom::Mel)
    //     .create_height_range(900_000..)
    //     .unspent()
    //     .iter()
    // {
    //     *sum.entry(item.coin_data.covhash).or_default() += item.coin_data.value;
    // }
    // for (a, b) in sum {
    //     println!("{}..,{}", &a.to_string()[..10], b);
    // }
    // println!("height,fee_pool,fee_multiplier,dosc_speed,mel_balance,sym_balance,tx_count");
    // let mel_balance = indexer.query_coins().denom(Denom::Mel).balance_tracker();
    // let sym_balance = indexer.query_coins().denom(Denom::Sym).balance_tracker();
    // for height in 1..1200000 / 1000 {
    //     let height = height * 1000;
    //     let info = indexer.height_info(height).unwrap();
    //     println!(
    //         "{},{},{},{},{},{},{}",
    //         height,
    //         CoinValue(info.fee_pool),
    //         info.fee_multiplier,
    //         info.dosc_speed,
    //         mel_balance.balance_at(height).unwrap() + CoinValue(info.fee_pool),
    //         sym_balance.balance_at(height).unwrap(),
    //         indexer
    //             .query_coins()
    //             .create_height_range(height..=height)
    //             .iter()
    //             .map(|d| d.create_txhash)
    //             .unique()
    //             .count()
    //     );
    // }
    loop {
        std::thread::sleep(Duration::from_secs(5));
        // compute balance
        let start = Instant::now();
        let sum: f64 = indexer
            .query_coins()
            .unspent()
            .denom(Denom::Mel)
            .iter()
            .map(|d| d.coin_data.value.0 as f64 / 1_000_000.0)
            .sum();
        eprintln!("{} MEL in circulation {:?}", sum, start.elapsed());
    }
}
