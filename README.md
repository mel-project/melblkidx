# Block indexer for Melscan (and possibly more)

The block indexer pulls blocks from the network one by one using a `Client` and indexes them.

## SQLite schema overview

### `coins` table

The main data structure is a SQLite table that tracks the state of all coins seen with the following columns:

- `create_txhash`: hash of the creating transaction
- `create_index`: index at the creating transaction (e.g. which output was it)
- `create_height`: height of the creating transaction
- `spend_txhash`: hash of the spending transaction
- `spend_index`: index at the spending transaction (e.g. which input was it)
- `spend_height`: height of the spending transaction
- `value`: value being spent (big-endian blob)
- `denom`: denomination (blob)
- `covhash`: address (string)
- `additional_data`: (blob)

The spending information is denormalized into this table to improve the performance of e.g. balance queries ("which coins were created")

"Tricky" cases (Melswap transactions, mostly) are handled by actually asking for more information from the network, rather than by reimplementating all the nuances of the state transition function. This is a big reason why we use a pull-based rather than an I/O-free, push-based API.

### `headvars` table

- `height`
- `blkhash`
- `fee_pool`
- `fee_multiplier`
- `dosc_speed`

### `stakes` table

stores _all stakedocs ever_, which is all we really need

- `txhash`
- `pubkey`
- `e_start`
- `e_post_end`
- `staked`

### `txvars` table

- `txhash`
- `kind`
- `fee`
- `covenants` (JSON)
- `data`
- `sigs` (JSON)

## Query API

### Query facts about coins

```rust
// iterator over all coins with value above 100 µMEL
let i: impl Iterator<Item = CoinInfo> =
    indexer.query_coins()
        .value_range(CoinValue(100)..)
        .denom(Denom::Mel)
        .iter()
```

```rust
pub struct CoinInfo {
    pub create_txhash: TxHash,
    pub create_index: u8,
    pub create_height: BlockHeight,
    pub coin_data: CoinData,
    pub spend_info: Option<CoinSpendInfo>
}

pub struct CoinSpendInfo {
    pub spend_txhash: TxHash,
    pub spend_index: u8,
    pub spend_height: BlockHeight
}
```
