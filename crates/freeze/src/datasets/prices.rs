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
const PRICE_QUERY_CALLDATA: &str = "608060405234801561001057600080fd5b50600061003673deb288f737066589598e9214e782fa5a8ed689e861061760201b60201c565b6ec097ce7bc90715b34b9f100000000061005091906106f7565b61007373986b5e1e1755e3c2440e960477f25201b0a8bbd461061760201b60201c565b6ec097ce7bc90715b34b9f100000000061008d91906106f7565b6100b073773616e4d11a78f511299002da57a0a94577f1f461061760201b60201c565b6ec097ce7bc90715b34b9f10000000006100ca91906106f7565b6100ed73ee9f2375b4bdf6387aa8265dd4fb8f16512a1d4661061760201b60201c565b6ec097ce7bc90715b34b9f100000000061010791906106f7565b61012a73dc530d9457755926550b59e8eccdae762418155761061760201b60201c565b6ec097ce7bc90715b34b9f100000000061014491906106f7565b6101677324551a8fb2a7211a25a17b1481f043a8a8adc7f261061760201b60201c565b6ec097ce7bc90715b34b9f100000000061018191906106f7565b60405160200161019696959493929190610737565b604051602081830303815290604052905060006101cc738e0b7e6062272b5ef4524250bfff8e5bd349775761061760201b60201c565b6ec097ce7bc90715b34b9f10000000006101e691906106f7565b6102097379291a9d692df95334b1a0b3b4ae6bc606782f8c61061760201b60201c565b6ec097ce7bc90715b34b9f100000000061022391906106f7565b610246732de7e4a9488488e0058b95854cc2f7955b35dc9b61061760201b60201c565b6ec097ce7bc90715b34b9f100000000061026091906106f7565b610283737c5d4f8345e66f68099581db340cd65b078c41f461061760201b60201c565b6ec097ce7bc90715b34b9f100000000061029d91906106f7565b6102c0731b39ee86ec5979ba5c322b826b3ecb8c7999169961061760201b60201c565b6ec097ce7bc90715b34b9f10000000006102da91906106f7565b6102fd738a12be339b0cd1829b91adc01977caa5e9ac121e61061760201b60201c565b6ec097ce7bc90715b34b9f100000000061031791906106f7565b60405160200161032c96959493929190610737565b60405160208183030381529060405290506000610362736df09e975c830ecae5bd4ed9d90f3a95a4f8801261061760201b60201c565b6ec097ce7bc90715b34b9f100000000061037c91906106f7565b61039f73c1438aa3823a6ba0c159cfa8d98df5a994ba120b61061760201b60201c565b6ec097ce7bc90715b34b9f10000000006103b991906106f7565b6103dc73d6aa3d25116d8da79ea0246c4826eb951872e02e61061760201b60201c565b6ec097ce7bc90715b34b9f10000000006103f691906106f7565b61041973e572cef69f43c2e488b33924af04bdace19079cf61061760201b60201c565b6ec097ce7bc90715b34b9f100000000061043391906106f7565b6104567314d04fff8d21bd62987a5ce9ce543d2f1edf5d3e61061760201b60201c565b6ec097ce7bc90715b34b9f100000000061047091906106f7565b6104937386392dc19c0b719886221c78ab11eb8cf5c5281261061760201b60201c565b6ec097ce7bc90715b34b9f10000000006104ad91906106f7565b6104d073536218f9e9eb48863970252233c8f271f554c2d061061760201b60201c565b6ec097ce7bc90715b34b9f10000000006104ea91906106f7565b6040516020016105009796959493929190610798565b60405160208183030381529060405290506000610536735f4ec3df9cbd43714fe2740f5e3616155c5b841961061760201b60201c565b9050600061055d73d10abbc76679a20055e167bb80a24ac851b3705661061760201b60201c565b670de0b6b3a7640000836105719190610807565b61057b91906106f7565b905060006105a2737bac85a8a13a4bcd8abb3eb7d6b4d632c5a5767661061760201b60201c565b670de0b6b3a7640000846105b69190610807565b6105c091906106f7565b9050600086868685856040516020016105da929190610849565b6040516020818303038152906040526040516020016105fc94939291906108e3565b60405160208183030381529060405290506020810180590381f35b60008173ffffffffffffffffffffffffffffffffffffffff166350d25bcd6040518163ffffffff1660e01b8152600401602060405180830381865afa158015610664573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190610688919061095c565b9050919050565b6000819050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b60006107028261068f565b915061070d8361068f565b92508261071d5761071c610699565b5b828204905092915050565b6107318161068f565b82525050565b600060c08201905061074c6000830189610728565b6107596020830188610728565b6107666040830187610728565b6107736060830186610728565b6107806080830185610728565b61078d60a0830184610728565b979650505050505050565b600060e0820190506107ad600083018a610728565b6107ba6020830189610728565b6107c76040830188610728565b6107d46060830187610728565b6107e16080830186610728565b6107ee60a0830185610728565b6107fb60c0830184610728565b98975050505050505050565b60006108128261068f565b915061081d8361068f565b925082820261082b8161068f565b91508282048414831517610842576108416106c8565b5b5092915050565b600060408201905061085e6000830185610728565b61086b6020830184610728565b9392505050565b600081519050919050565b600081905092915050565b60005b838110156108a657808201518184015260208101905061088b565b60008484015250505050565b60006108bd82610872565b6108c7818561087d565b93506108d7818560208601610888565b80840191505092915050565b60006108ef82876108b2565b91506108fb82866108b2565b915061090782856108b2565b915061091382846108b2565b915081905095945050505050565b600080fd5b6000819050919050565b61093981610926565b811461094457600080fd5b50565b60008151905061095681610930565b92915050565b60006020828403121561097257610971610921565b5b600061098084828501610947565b9150509291505056fe";

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
            ("eth_btc", ColumnType::Float64),
            ("eth_usdc", ColumnType::Float64),
            ("eth_dai", ColumnType::Float64),
            ("eth_usdt", ColumnType::Float64),
            ("eth_link", ColumnType::Float64),
            ("eth_mkr", ColumnType::Float64),
            ("eth_susd", ColumnType::Float64),
            ("eth_snx", ColumnType::Float64),
            ("eth_ftm", ColumnType::Float64),
            ("eth_yfi", ColumnType::Float64),
            ("eth_comp", ColumnType::Float64),
            ("eth_crv", ColumnType::Float64),
            ("eth_aave", ColumnType::Float64),
            ("eth_bal", ColumnType::Float64),
            ("eth_uni", ColumnType::Float64),
            ("eth_sushi", ColumnType::Float64),
            ("eth_frax", ColumnType::Float64),
            ("eth_steth", ColumnType::Float64),
            ("eth_reth", ColumnType::Float64),
            ("eth_ape", ColumnType::Float64),
            ("eth_matic", ColumnType::Float64),
        ])
    }

    fn default_columns(&self) -> Vec<&'static str> {
        vec![
            "block_number",
            "eth_btc",
            "eth_usdc",
            "eth_dai",
            "eth_usdt",
            "eth_link",
            "eth_mkr",
            "eth_susd",
            "eth_snx",
            "eth_ftm",
            "eth_yfi",
            "eth_comp",
            "eth_crv",
            "eth_aave",
            "eth_bal",
            "eth_uni",
            "eth_sushi",
            "eth_frax",
            "eth_steth",
            "eth_reth",
            "eth_ape",
            "eth_matic",
        ]
    }

    fn default_sort(&self) -> Vec<String> {
        vec![
            "block_number".to_string(),
            "eth_btc".to_string(),
            "eth_usdc".to_string(),
            "eth_dai".to_string(),
            "eth_usdt".to_string(),
            "eth_link".to_string(),
            "eth_mkr".to_string(),
            "eth_susd".to_string(),
            "eth_snx".to_string(),
            "eth_ftm".to_string(),
            "eth_yfi".to_string(),
            "eth_comp".to_string(),
            "eth_crv".to_string(),
            "eth_aave".to_string(),
            "eth_bal".to_string(),
            "eth_uni".to_string(),
            "eth_sushi".to_string(),
            "eth_frax".to_string(),
            "eth_steth".to_string(),
            "eth_reth".to_string(),
            "eth_ape".to_string(),
            "eth_matic".to_string(),
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
) -> mpsc::Receiver<Result<BlockOracle, CollectError>> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let call_data: Vec<u8> = hex::decode(PRICE_QUERY_CALLDATA).unwrap();
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
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
                            ParamType::Uint(256),
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
                    let eth_btc = format_ether(price_res[0]).parse::<f64>().unwrap();
                    let eth_usdc = format_ether(price_res[1]).parse::<f64>().unwrap();
                    let eth_dai = format_ether(price_res[2]).parse::<f64>().unwrap();
                    let eth_usdt = format_ether(price_res[3]).parse::<f64>().unwrap();
                    let eth_link = format_ether(price_res[4]).parse::<f64>().unwrap();
                    let eth_mkr = format_ether(price_res[5]).parse::<f64>().unwrap();
                    let eth_susd = format_ether(price_res[6]).parse::<f64>().unwrap();
                    let eth_snx = format_ether(price_res[7]).parse::<f64>().unwrap();
                    let eth_ftm = format_ether(price_res[8]).parse::<f64>().unwrap();
                    let eth_yfi = format_ether(price_res[9]).parse::<f64>().unwrap();
                    let eth_comp = format_ether(price_res[10]).parse::<f64>().unwrap();
                    let eth_crv = format_ether(price_res[11]).parse::<f64>().unwrap();
                    let eth_aave = format_ether(price_res[12]).parse::<f64>().unwrap();
                    let eth_bal = format_ether(price_res[13]).parse::<f64>().unwrap();
                    let eth_uni = format_ether(price_res[14]).parse::<f64>().unwrap();
                    let eth_sushi = format_ether(price_res[15]).parse::<f64>().unwrap();
                    let eth_frax = format_ether(price_res[16]).parse::<f64>().unwrap();
                    let eth_steth = format_ether(price_res[17]).parse::<f64>().unwrap();
                    let eth_reth = format_ether(price_res[18]).parse::<f64>().unwrap();
                    let eth_ape = format_ether(price_res[19]).parse::<f64>().unwrap();
                    let eth_matic = format_ether(price_res[20]).parse::<f64>().unwrap();
                    Ok(BlockWithPrices {
                        block_number: number as u32,
                        eth_btc,
                        eth_usdc,
                        eth_dai,
                        eth_usdt,
                        eth_link,
                        eth_mkr,
                        eth_susd,
                        eth_snx,
                        eth_ftm,
                        eth_yfi,
                        eth_comp,
                        eth_crv,
                        eth_aave,
                        eth_bal,
                        eth_uni,
                        eth_sushi,
                        eth_frax,
                        eth_steth,
                        eth_reth,
                        eth_ape,
                        eth_matic,
                    })
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
    mut prices: mpsc::Receiver<Result<BlockOracle, CollectError>>,
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

pub struct BlockWithPrices {
    block_number: u32,
    eth_btc: f64,
    eth_usdc: f64,
    eth_dai: f64,
    eth_usdt: f64,
    eth_link: f64,
    eth_mkr: f64,
    eth_susd: f64,
    eth_snx: f64,
    eth_ftm: f64,
    eth_yfi: f64,
    eth_comp: f64,
    eth_crv: f64,
    eth_aave: f64,
    eth_bal: f64,
    eth_uni: f64,
    eth_sushi: f64,
    eth_frax: f64,
    eth_steth: f64,
    eth_reth: f64,
    eth_ape: f64,
    eth_matic: f64,
}

pub(crate) type BlockOracle = BlockWithPrices;

#[derive(Default)]
struct PricesColumns {
    n_rows: usize,
    block_number: Vec<u32>,
    eth_btc: Vec<f64>,
    eth_usdc: Vec<f64>,
    eth_dai: Vec<f64>,
    eth_usdt: Vec<f64>,
    eth_link: Vec<f64>,
    eth_mkr: Vec<f64>,
    eth_susd: Vec<f64>,
    eth_snx: Vec<f64>,
    eth_ftm: Vec<f64>,
    eth_yfi: Vec<f64>,
    eth_comp: Vec<f64>,
    eth_crv: Vec<f64>,
    eth_aave: Vec<f64>,
    eth_bal: Vec<f64>,
    eth_uni: Vec<f64>,
    eth_sushi: Vec<f64>,
    eth_frax: Vec<f64>,
    eth_steth: Vec<f64>,
    eth_reth: Vec<f64>,
    eth_ape: Vec<f64>,
    eth_matic: Vec<f64>,
}

impl PricesColumns {
    fn process_price(&mut self, prices: BlockOracle, schema: &Table) {
        self.n_rows += 1;
        if schema.has_column("block_number") {
            self.block_number.push(prices.block_number);
        }
        if schema.has_column("eth_btc") {
            self.eth_btc.push(prices.eth_btc);
        }
        if schema.has_column("eth_usdc") {
            self.eth_usdc.push(prices.eth_usdc);
        }
        if schema.has_column("eth_dai") {
            self.eth_dai.push(prices.eth_dai);
        }
        if schema.has_column("eth_usdt") {
            self.eth_usdt.push(prices.eth_usdt);
        }
        if schema.has_column("eth_link") {
            self.eth_link.push(prices.eth_link);
        }
        if schema.has_column("eth_mkr") {
            self.eth_mkr.push(prices.eth_mkr);
        }
        if schema.has_column("eth_susd") {
            self.eth_susd.push(prices.eth_susd);
        }
        if schema.has_column("eth_snx") {
            self.eth_snx.push(prices.eth_snx);
        }
        if schema.has_column("eth_ftm") {
            self.eth_ftm.push(prices.eth_ftm);
        }
        if schema.has_column("eth_yfi") {
            self.eth_yfi.push(prices.eth_yfi);
        }
        if schema.has_column("eth_comp") {
            self.eth_comp.push(prices.eth_comp);
        }
        if schema.has_column("eth_crv") {
            self.eth_crv.push(prices.eth_crv);
        }
        if schema.has_column("eth_aave") {
            self.eth_aave.push(prices.eth_aave);
        }
        if schema.has_column("eth_bal") {
            self.eth_bal.push(prices.eth_bal);
        }
        if schema.has_column("eth_uni") {
            self.eth_uni.push(prices.eth_uni);
        }
        if schema.has_column("eth_sushi") {
            self.eth_sushi.push(prices.eth_sushi);
        }
        if schema.has_column("eth_frax") {
            self.eth_frax.push(prices.eth_frax);
        }
        if schema.has_column("eth_steth") {
            self.eth_steth.push(prices.eth_steth);
        }
        if schema.has_column("eth_reth") {
            self.eth_reth.push(prices.eth_reth);
        }
        if schema.has_column("eth_ape") {
            self.eth_ape.push(prices.eth_ape);
        }
        if schema.has_column("eth_matic") {
            self.eth_matic.push(prices.eth_matic);
        }
    }

    fn create_df(self, schema: &Table, _chain_id: u64) -> Result<DataFrame, CollectError> {
        let mut cols = Vec::with_capacity(schema.columns().len());
        with_series!(cols, "block_number", self.block_number, schema);
        with_series!(cols, "eth_btc", self.eth_btc, schema);
        with_series!(cols, "eth_usdc", self.eth_usdc, schema);
        with_series!(cols, "eth_dai", self.eth_dai, schema);
        with_series!(cols, "eth_usdt", self.eth_usdt, schema);
        with_series!(cols, "eth_link", self.eth_link, schema);
        with_series!(cols, "eth_mkr", self.eth_mkr, schema);
        with_series!(cols, "eth_susd", self.eth_susd, schema);
        with_series!(cols, "eth_snx", self.eth_snx, schema);
        with_series!(cols, "eth_ftm", self.eth_ftm, schema);
        with_series!(cols, "eth_yfi", self.eth_yfi, schema);
        with_series!(cols, "eth_comp", self.eth_comp, schema);
        with_series!(cols, "eth_crv", self.eth_crv, schema);
        with_series!(cols, "eth_aave", self.eth_aave, schema);
        with_series!(cols, "eth_bal", self.eth_bal, schema);
        with_series!(cols, "eth_uni", self.eth_uni, schema);
        with_series!(cols, "eth_sushi", self.eth_sushi, schema);
        with_series!(cols, "eth_frax", self.eth_frax, schema);
        with_series!(cols, "eth_steth", self.eth_steth, schema);
        with_series!(cols, "eth_reth", self.eth_reth, schema);
        with_series!(cols, "eth_ape", self.eth_ape, schema);
        with_series!(cols, "eth_matic", self.eth_matic, schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
