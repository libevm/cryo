// required args:: address

use crate::{types::Prices, ColumnType, Dataset, Datatype};
use ethers_core::utils::format_ether;
use std::collections::HashMap;

use ethers::abi::{decode, ParamType};
use ethers::prelude::*;
use ethers::utils::hex;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use crate::{
    dataframes::SortableDataFrame,
    types::{BlockChunk, CollectError, RowFilter, Source, Table},
    with_series,
};

// https://github.com/libevm/eth_call_abuser
// Custom contract with return data on creation
// Could probably upgrade RETH node and add a custom API? Whatever is easier lol
const PRIC_QUERY_CALLDATA: &str = "608060405234801561001057600080fd5b50600061005473bb2b8038a1640196fbe3e38816f3e67cba72d94073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006101a060201b60201c565b9050600061009973b4e16d0168e52d35cacd2c6185b44281ec28c9dc73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006101a060201b60201c565b905060006100de73a478c2975ab1ea89e8196811f51a7b7ade33eb1173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006101a060201b60201c565b90506000610123730d4a11d5eeaac28ec3f61d100daf4d40471f185273c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006101a060201b60201c565b9050600061016873a2107fa5b38d9bbd2c461d6edf11b11a50f6b97473c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006101a060201b60201c565b905060008585858585604051602001610185959493929190610504565b60405160208183030381529060405290506020810180590381f35b60008084905060008173ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa1580156101f3573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061021791906105ba565b905060008273ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa158015610266573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061028a91906105ba565b90506000808473ffffffffffffffffffffffffffffffffffffffff16630902f1ac6040518163ffffffff1660e01b8152600401606060405180830381865afa1580156102da573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906102fe9190610669565b506dffffffffffffffffffffffffffff1691506dffffffffffffffffffffffffffff16915060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa158015610370573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061039491906106e8565b905060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa1580156103e3573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061040791906106e8565b90508473ffffffffffffffffffffffffffffffffffffffff168a73ffffffffffffffffffffffffffffffffffffffff16036104925780600a6104499190610877565b8461045491906108c2565b935081600a6104639190610877565b83858b61047091906108c2565b61047a9190610933565b6104849190610933565b9750505050505050506104e4565b81600a61049f9190610877565b836104aa91906108c2565b925080600a6104b99190610877565b84848b6104c691906108c2565b6104d09190610933565b6104da9190610933565b9750505050505050505b9392505050565b6000819050919050565b6104fe816104eb565b82525050565b600060a08201905061051960008301886104f5565b61052660208301876104f5565b61053360408301866104f5565b61054060608301856104f5565b61054d60808301846104f5565b9695505050505050565b600080fd5b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b60006105878261055c565b9050919050565b6105978161057c565b81146105a257600080fd5b50565b6000815190506105b48161058e565b92915050565b6000602082840312156105d0576105cf610557565b5b60006105de848285016105a5565b91505092915050565b60006dffffffffffffffffffffffffffff82169050919050565b61060a816105e7565b811461061557600080fd5b50565b60008151905061062781610601565b92915050565b600063ffffffff82169050919050565b6106468161062d565b811461065157600080fd5b50565b6000815190506106638161063d565b92915050565b60008060006060848603121561068257610681610557565b5b600061069086828701610618565b93505060206106a186828701610618565b92505060406106b286828701610654565b9150509250925092565b6106c5816104eb565b81146106d057600080fd5b50565b6000815190506106e2816106bc565b92915050565b6000602082840312156106fe576106fd610557565b5b600061070c848285016106d3565b91505092915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b60008160011c9050919050565b6000808291508390505b600185111561079b5780860481111561077757610776610715565b5b60018516156107865780820291505b808102905061079485610744565b945061075b565b94509492505050565b6000826107b45760019050610870565b816107c25760009050610870565b81600181146107d857600281146107e257610811565b6001915050610870565b60ff8411156107f4576107f3610715565b5b8360020a91508482111561080b5761080a610715565b5b50610870565b5060208310610133831016604e8410600b84101617156108465782820a90508381111561084157610840610715565b5b610870565b6108538484846001610751565b9250905081840481111561086a57610869610715565b5b81810290505b9392505050565b6000610882826104eb565b915061088d836104eb565b92506108ba7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff84846107a4565b905092915050565b60006108cd826104eb565b91506108d8836104eb565b92508282026108e6816104eb565b915082820484148315176108fd576108fc610715565b5b5092915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b600061093e826104eb565b9150610949836104eb565b92508261095957610958610904565b5b82820490509291505056fe";

#[async_trait::async_trait]
impl Dataset for Prices {
    fn datatype(&self) -> Datatype {
        Datatype::Prices
    }

    fn name(&self) -> &'static str {
        "prices"
    }

    fn column_types(&self) -> HashMap<&'static str, ColumnType> {
        // Note that the prices
        HashMap::from_iter(vec![
            ("block_number", ColumnType::UInt32),
            ("weth_wbtc", ColumnType::Float64),
            ("weth_usdc", ColumnType::Float64),
            ("weth_dai", ColumnType::Float64),
            ("weth_usdt", ColumnType::Float64),
            ("weth_link", ColumnType::Float64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["block_number", "weth_wbtc", "weth_usdc", "weth_dai", "weth_usdt", "weth_link"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec![
            "block_number".to_string(),
            "weth_wbtc".to_string(),
            "weth_usdc".to_string(),
            "weth_dai".to_string(),
            "weth_usdt".to_string(),
            "weth_link".to_string(),
        ]
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schema: &Table,
        _filter: Option<&RowFilter>,
    ) -> Result<DataFrame, CollectError> {
        let rx = fetch_prices(chunk, source).await;
        prices_to_df(rx, schema, source.chain_id).await
    }
}

async fn fetch_prices(
    block_chunk: &BlockChunk,
    source: &Source,
) -> mpsc::Receiver<Result<BlockWbtcUsdcDaiUsdt, CollectError>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let call_data: Vec<u8> = hex::decode(PRIC_QUERY_CALLDATA).unwrap();
        let fetcher = source.fetcher.clone();
        task::spawn(async move {
            let transaction =
                TransactionRequest { data: Some(call_data.clone().into()), ..Default::default() };

            let result = fetcher.call(transaction, number.into()).await;
            let result = match result {
                Ok(value) => {
                    let price_res: Vec<U256> = decode(
                        &[
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256)
                        ],
                        &value,
                    )
                    .unwrap()
                    .into_iter()
                    .map(|x| x.into_uint().unwrap())
                    .collect();
                    let weth_wbtc = format_ether(price_res[0]).parse::<f64>().unwrap();
                    let weth_usdc = format_ether(price_res[1]).parse::<f64>().unwrap();
                    let weth_dai = format_ether(price_res[2]).parse::<f64>().unwrap();
                    let weth_usdt = format_ether(price_res[3]).parse::<f64>().unwrap();
                    let weth_link = format_ether(price_res[4]).parse::<f64>().unwrap();
                    Ok((number as u32, weth_wbtc, weth_usdc, weth_dai, weth_usdt, weth_link))
                }
                Err(e) => Err(e),
            };

            match tx.send(result).await {
                Ok(_) => {}
                Err(tokio::sync::mpsc::error::SendError(_e)) => {
                    eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                    std::process::exit(1)
                }
            }
        });
    }

    rx
}

async fn prices_to_df(
    mut prices: mpsc::Receiver<Result<BlockWbtcUsdcDaiUsdt, CollectError>>,
    schema: &Table,
    chain_id: u64,
) -> Result<DataFrame, CollectError> {
    let mut columns = PricesColumns::default();
    while let Some(message) = prices.recv().await {
        match message {
            Ok(prices) => {
                columns.process_price(prices, schema);
            }
            Err(e) => {
                println!("{:?}", e);
                return Err(CollectError::TooManyRequestsError);
            }
        }
    }

    columns.create_df(schema, chain_id)
}

pub(crate) type BlockWbtcUsdcDaiUsdt = (u32, f64, f64, f64, f64, f64);

#[derive(Default)]
struct PricesColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    weth_wbtc: Vec<f64>,
    weth_usdc: Vec<f64>,
    weth_dai: Vec<f64>,
    weth_usdt: Vec<f64>,
    weth_link: Vec<f64>,
}

impl PricesColumns {
    fn process_price(&mut self, prices: BlockWbtcUsdcDaiUsdt, schema: &Table) {
        let (block_number, weth_wbtc, weth_usdc, weth_dai, weth_usdt, weth_link) = prices;
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(block_number);
        }
        if schema.has_column("weth_wbtc") {
            self.weth_wbtc.push(weth_wbtc);
        }
        if schema.has_column("weth_usdc") {
            self.weth_usdc.push(weth_usdc);
        }
        if schema.has_column("weth_dai") {
            self.weth_dai.push(weth_dai);
        }
        if schema.has_column("weth_usdt") {
            self.weth_usdt.push(weth_usdt);
        }
        if schema.has_column("weth_link") {
            self.weth_link.push(weth_link);
        }
    }

    fn create_df(self, schema: &Table, _chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series!(cols, "weth_wbtc", self.weth_wbtc, schema);
        with_series!(cols, "weth_usdc", self.weth_usdc, schema);
        with_series!(cols, "weth_dai", self.weth_dai, schema);
        with_series!(cols, "weth_usdt", self.weth_usdt, schema);
        with_series!(cols, "weth_link", self.weth_link, schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
