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
const PRIC_QUERY_CALLDATA: &str = "608060405234801561001057600080fd5b50600061005473bb2b8038a1640196fbe3e38816f3e67cba72d94073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061015960201b60201c565b9050600061009973b4e16d0168e52d35cacd2c6185b44281ec28c9dc73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061015960201b60201c565b905060006100de73a478c2975ab1ea89e8196811f51a7b7ade33eb1173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061015960201b60201c565b90506000610123730d4a11d5eeaac28ec3f61d100daf4d40471f185273c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061015960201b60201c565b905060008484848460405160200161013e94939291906104bd565b60405160208183030381529060405290506020810180590381f35b60008084905060008173ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa1580156101ac573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906101d09190610565565b905060008273ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa15801561021f573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906102439190610565565b90506000808473ffffffffffffffffffffffffffffffffffffffff16630902f1ac6040518163ffffffff1660e01b8152600401606060405180830381865afa158015610293573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906102b79190610614565b506dffffffffffffffffffffffffffff1691506dffffffffffffffffffffffffffff16915060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa158015610329573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061034d9190610693565b905060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa15801561039c573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906103c09190610693565b90508473ffffffffffffffffffffffffffffffffffffffff168a73ffffffffffffffffffffffffffffffffffffffff160361044b5780600a6104029190610822565b8461040d919061086d565b935081600a61041c9190610822565b83858b610429919061086d565b61043391906108de565b61043d91906108de565b97505050505050505061049d565b81600a6104589190610822565b83610463919061086d565b925080600a6104729190610822565b84848b61047f919061086d565b61048991906108de565b61049391906108de565b9750505050505050505b9392505050565b6000819050919050565b6104b7816104a4565b82525050565b60006080820190506104d260008301876104ae565b6104df60208301866104ae565b6104ec60408301856104ae565b6104f960608301846104ae565b95945050505050565b600080fd5b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b600061053282610507565b9050919050565b61054281610527565b811461054d57600080fd5b50565b60008151905061055f81610539565b92915050565b60006020828403121561057b5761057a610502565b5b600061058984828501610550565b91505092915050565b60006dffffffffffffffffffffffffffff82169050919050565b6105b581610592565b81146105c057600080fd5b50565b6000815190506105d2816105ac565b92915050565b600063ffffffff82169050919050565b6105f1816105d8565b81146105fc57600080fd5b50565b60008151905061060e816105e8565b92915050565b60008060006060848603121561062d5761062c610502565b5b600061063b868287016105c3565b935050602061064c868287016105c3565b925050604061065d868287016105ff565b9150509250925092565b610670816104a4565b811461067b57600080fd5b50565b60008151905061068d81610667565b92915050565b6000602082840312156106a9576106a8610502565b5b60006106b78482850161067e565b91505092915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b60008160011c9050919050565b6000808291508390505b600185111561074657808604811115610722576107216106c0565b5b60018516156107315780820291505b808102905061073f856106ef565b9450610706565b94509492505050565b60008261075f576001905061081b565b8161076d576000905061081b565b8160018114610783576002811461078d576107bc565b600191505061081b565b60ff84111561079f5761079e6106c0565b5b8360020a9150848211156107b6576107b56106c0565b5b5061081b565b5060208310610133831016604e8410600b84101617156107f15782820a9050838111156107ec576107eb6106c0565b5b61081b565b6107fe84848460016106fc565b92509050818404811115610815576108146106c0565b5b81810290505b9392505050565b600061082d826104a4565b9150610838836104a4565b92506108657fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff848461074f565b905092915050565b6000610878826104a4565b9150610883836104a4565b9250828202610891816104a4565b915082820484148315176108a8576108a76106c0565b5b5092915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b60006108e9826104a4565b91506108f4836104a4565b925082610904576109036108af565b5b82820490509291505056fe";

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
            ("weth_wbtc", ColumnType::Float64),
            ("weth_usdc", ColumnType::Float64),
            ("weth_dai", ColumnType::Float64),
            ("weth_usdt", ColumnType::Float64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec!["weth_wbtc", "weth_usdc", "weth_dai", "weth_usdt"]
    }

    fn default_sort(&self) -> Vec<String> {
        vec![
            "weth_wbtc".to_string(),
            "weth_usdc".to_string(),
            "weth_dai".to_string(),
            "weth_usdt".to_string(),
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
) -> mpsc::Receiver<Result<WbtcUsdcDaiUsdt, CollectError>> {
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
                    Ok((weth_wbtc, weth_usdc, weth_dai, weth_usdt))
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
    mut prices: mpsc::Receiver<Result<WbtcUsdcDaiUsdt, CollectError>>,
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

pub(crate) type WbtcUsdcDaiUsdt = (f64, f64, f64, f64);

#[derive(Default)]
struct PricesColumns {
    n_rows: usize,
    weth_wbtc: Vec<f64>,
    weth_usdc: Vec<f64>,
    weth_dai: Vec<f64>,
    weth_usdt: Vec<f64>,
}

impl PricesColumns {
    fn process_price(&mut self, prices: WbtcUsdcDaiUsdt, schema: &Table) {
        let (weth_wbtc, weth_usdc, weth_dai, weth_usdt) = prices;
        self.n_rows += 1;
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
    }

    fn create_df(self, schema: &Table, _chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "weth_wbtc", self.weth_wbtc, schema);
        with_series!(cols, "weth_usdc", self.weth_usdc, schema);
        with_series!(cols, "weth_dai", self.weth_dai, schema);
        with_series!(cols, "weth_usdt", self.weth_usdt, schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
