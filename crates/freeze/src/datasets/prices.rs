use crate::*;
use ethers::abi::{decode, ParamType};
use ethers::prelude::*;
use ethers::utils::hex;
use ethers_core::utils::format_ether;
use polars::prelude::*;

// https://github.com/libevm/eth_call_abuser
const PRICE_QUERY_CALLDATA: &str = "60a060405262ef902043116100285773d340b57aacdd10f96fc1cf10e15921936f41e29c61003e565b73109830a1aaad605bbf02a9dfa7b0b92ec2fb7daa5b73ffffffffffffffffffffffffffffffffffffffff1660809073ffffffffffffffffffffffffffffffffffffffff1681525034801561007c57600080fd5b5060006100b77388e6a0c2ddd26feeb64f039a2c41296fcb3f564073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261041b60201b60201c565b905060006100f373c63b0708e2f7e69cb8a1df0e1389a98c35a76d5273a0b86991c6218b36c1d19d4a2e9eb0ce3606eb4861041b60201b60201c565b90506000670de0b6b3a7640000828461010c9190610ccf565b6101169190610d40565b905060006001905061014173a1f8a6807c402e4a15ef4eba36528a3fed24e5776107c060201b60201c565b1561029757678ac7230489e80000735e8422345238f34275888049021821e8e08caa1f73ffffffffffffffffffffffffffffffffffffffff166370a0823173a1f8a6807c402e4a15ef4eba36528a3fed24e5776040518263ffffffff1660e01b81526004016101b09190610db2565b602060405180830381865afa1580156101cd573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906101f19190610dfe565b11156102965773a1f8a6807c402e4a15ef4eba36528a3fed24e57773ffffffffffffffffffffffffffffffffffffffff16635e0d443f60006001670de0b6b3a76400006040518463ffffffff1660e01b815260040161025293929190610ef3565b602060405180830381865afa15801561026f573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906102939190610dfe565b90505b5b6000846102d2734e68ccd3e89f51c3074ca5072bbac773960dfa3673c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261041b60201b60201c565b61030a7360594a405d53811d3bc4766596efd80fd545a27073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261041b60201b60201c565b61034273cbcdf9626bc03e24f779434178a73a0b4bad62ed73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261041b60201b60201c565b610383733fd4cf9303c4bc9e13772618828712c8eac7dd2f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006107d360201b60201c565b87876103ab60805173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261041b60201b60201c565b6103e373a4e0faa58465a2d369aa21b3e42d43374c6f961373c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261041b60201b60201c565b6040516020016103fb99989796959493929190610f39565b604051602081830303815290604052905060008190506020810180590381f35b600061042c836107c060201b60201c565b61043957600190506107ba565b600083905060008173ffffffffffffffffffffffffffffffffffffffff16633850c7bd6040518163ffffffff1660e01b815260040160e060405180830381865afa15801561048b573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906104af91906110d6565b50505050505090506401000276a473ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff16111580610537575073fffd8963efd1fc6a506488495d951d5263988d2573ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff1610155b15610547576001925050506107ba565b60008273ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa158015610594573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906105b891906111a4565b905060008373ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa158015610607573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061062b91906111a4565b905060008273ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa15801561067a573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061069e9190610dfe565b905060008273ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa1580156106ed573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906107119190610dfe565b905060006107258684610b3b60201b60201c565b90508873ffffffffffffffffffffffffffffffffffffffff168573ffffffffffffffffffffffffffffffffffffffff160361078c5781601261076791906111d1565b600a6107739190611338565b8161077e9190610ccf565b9750505050505050506107ba565b8082601261079a9190611383565b600a6107a69190611338565b6107b09190610d40565b9750505050505050505b92915050565b600080823b905060008111915050919050565b60006107e4846107c060201b60201c565b6107f15760019050610b34565b600084905060008173ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa158015610843573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061086791906111a4565b905060008273ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa1580156108b6573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906108da91906111a4565b90506000808473ffffffffffffffffffffffffffffffffffffffff16630902f1ac6040518163ffffffff1660e01b8152600401606060405180830381865afa15801561092a573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061094e9190611439565b506dffffffffffffffffffffffffffff1691506dffffffffffffffffffffffffffff16915060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa1580156109c0573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906109e49190610dfe565b905060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa158015610a33573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190610a579190610dfe565b90508473ffffffffffffffffffffffffffffffffffffffff168a73ffffffffffffffffffffffffffffffffffffffff1603610ae25780600a610a999190611338565b84610aa49190610ccf565b935081600a610ab39190611338565b83858b610ac09190610ccf565b610aca9190610d40565b610ad49190610d40565b975050505050505050610b34565b81600a610aef9190611338565b83610afa9190610ccf565b925080600a610b099190611338565b84848b610b169190610ccf565b610b209190610d40565b610b2a9190610d40565b9750505050505050505b9392505050565b6000808373ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff16610b769190610ccf565b9050600083600a610b87919061148c565b9050610bb382827801000000000000000000000000000000000000000000000000610bbd60201b60201c565b9250505092915050565b6000806000801985870985870292508281108382030391505060008103610bf75760008411610beb57600080fd5b83820492505050610c8f565b808411610c0357600080fd5b600084868809905082811182039150808303925060008586600003169050808604955080840493506001818260000304019050808302841793506000600287600302189050808702600203810290508087026002038102905080870260020381029050808702600203810290508087026002038102905080870260020381029050808502955050505050505b9392505050565b6000819050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b6000610cda82610c96565b9150610ce583610c96565b9250828202610cf381610c96565b91508282048414831517610d0a57610d09610ca0565b5b5092915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b6000610d4b82610c96565b9150610d5683610c96565b925082610d6657610d65610d11565b5b828204905092915050565b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b6000610d9c82610d71565b9050919050565b610dac81610d91565b82525050565b6000602082019050610dc76000830184610da3565b92915050565b600080fd5b610ddb81610c96565b8114610de657600080fd5b50565b600081519050610df881610dd2565b92915050565b600060208284031215610e1457610e13610dcd565b5b6000610e2284828501610de9565b91505092915050565b6000819050919050565b600081600f0b9050919050565b6000819050919050565b6000610e67610e62610e5d84610e2b565b610e42565b610e35565b9050919050565b610e7781610e4c565b82525050565b6000819050919050565b6000610ea2610e9d610e9884610e7d565b610e42565b610e35565b9050919050565b610eb281610e87565b82525050565b6000819050919050565b6000610edd610ed8610ed384610eb8565b610e42565b610c96565b9050919050565b610eed81610ec2565b82525050565b6000606082019050610f086000830186610e6e565b610f156020830185610ea9565b610f226040830184610ee4565b949350505050565b610f3381610c96565b82525050565b600061012082019050610f4f600083018c610f2a565b610f5c602083018b610f2a565b610f69604083018a610f2a565b610f766060830189610f2a565b610f836080830188610f2a565b610f9060a0830187610f2a565b610f9d60c0830186610f2a565b610faa60e0830185610f2a565b610fb8610100830184610f2a565b9a9950505050505050505050565b610fcf81610d71565b8114610fda57600080fd5b50565b600081519050610fec81610fc6565b92915050565b60008160020b9050919050565b61100881610ff2565b811461101357600080fd5b50565b60008151905061102581610fff565b92915050565b600061ffff82169050919050565b6110428161102b565b811461104d57600080fd5b50565b60008151905061105f81611039565b92915050565b600060ff82169050919050565b61107b81611065565b811461108657600080fd5b50565b60008151905061109881611072565b92915050565b60008115159050919050565b6110b38161109e565b81146110be57600080fd5b50565b6000815190506110d0816110aa565b92915050565b600080600080600080600060e0888a0312156110f5576110f4610dcd565b5b60006111038a828b01610fdd565b97505060206111148a828b01611016565b96505060406111258a828b01611050565b95505060606111368a828b01611050565b94505060806111478a828b01611050565b93505060a06111588a828b01611089565b92505060c06111698a828b016110c1565b91505092959891949750929550565b61118181610d91565b811461118c57600080fd5b50565b60008151905061119e81611178565b92915050565b6000602082840312156111ba576111b9610dcd565b5b60006111c88482850161118f565b91505092915050565b60006111dc82610c96565b91506111e783610c96565b92508282039050818111156111ff576111fe610ca0565b5b92915050565b60008160011c9050919050565b6000808291508390505b600185111561125c5780860481111561123857611237610ca0565b5b60018516156112475780820291505b808102905061125585611205565b945061121c565b94509492505050565b6000826112755760019050611331565b816112835760009050611331565b816001811461129957600281146112a3576112d2565b6001915050611331565b60ff8411156112b5576112b4610ca0565b5b8360020a9150848211156112cc576112cb610ca0565b5b50611331565b5060208310610133831016604e8410600b84101617156113075782820a90508381111561130257611301610ca0565b5b611331565b6113148484846001611212565b9250905081840481111561132b5761132a610ca0565b5b81810290505b9392505050565b600061134382610c96565b915061134e83610c96565b925061137b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8484611265565b905092915050565b600061138e82610c96565b915061139983610c96565b92508282019050808211156113b1576113b0610ca0565b5b92915050565b60006dffffffffffffffffffffffffffff82169050919050565b6113da816113b7565b81146113e557600080fd5b50565b6000815190506113f7816113d1565b92915050565b600063ffffffff82169050919050565b611416816113fd565b811461142157600080fd5b50565b6000815190506114338161140d565b92915050565b60008060006060848603121561145257611451610dcd565b5b6000611460868287016113e8565b9350506020611471868287016113e8565b925050604061148286828701611424565b9150509250925092565b600061149782610c96565b91506114a283611065565b92506114cf7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8484611265565b90509291505056fe";

/// Columns for prices
#[cryo_to_df::to_df(Datatype::Prices)]
#[derive(Default)]
pub struct Prices {
    n_rows: usize,

    block_number: Vec<u32>,

    weth_usdc: Vec<f64>,
    weth_usdt: Vec<f64>,
    weth_dai: Vec<f64>,
    weth_wbtc: Vec<f64>,
    weth_bnt: Vec<f64>,
    weth_frax: Vec<f64>,
    weth_frxeth: Vec<f64>,
    weth_wsteth: Vec<f64>,
    weth_reth: Vec<f64>,

    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Prices {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number"])
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }
}

type Result<T> = ::core::result::Result<T, CollectError>;
type BlockPrices = (u32, Vec<f64>);

#[async_trait::async_trait]
impl CollectByBlock for Prices {
    type Response = BlockPrices;

    async fn extract(
        request: Params,
        source: Arc<Source>,
        _: Arc<Query>,
    ) -> Result<Self::Response> {
        let block_number = request.block_number()? as u32;

        let call_data: Vec<u8> = hex::decode(PRICE_QUERY_CALLDATA).unwrap();
        let transaction =
            TransactionRequest { data: Some(call_data.clone().into()), ..Default::default() };

        let output = source.fetcher.call(transaction, block_number.into()).await?;
        let resp: Vec<f64> = decode(
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
            ],
            &output,
        )
        .unwrap()
        .iter()
        .map(|x| format_ether(x.clone().into_uint().unwrap()).parse::<f64>().unwrap())
        .collect();

        Ok((block_number, resp))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> Result<()> {
        let schema = query.schemas.get_schema(&Datatype::Prices)?;
        process_prices(columns, response, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Prices {
    type Response = ();
}

fn process_prices(columns: &mut Prices, data: BlockPrices, schema: &Table) -> Result<()> {
    let (block, prices) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, weth_usdc, prices[0]);
    store!(schema, columns, weth_usdt, prices[1]);
    store!(schema, columns, weth_dai, prices[2]);
    store!(schema, columns, weth_wbtc, prices[3]);
    store!(schema, columns, weth_bnt, prices[4]);
    store!(schema, columns, weth_frax, prices[5]);
    store!(schema, columns, weth_frxeth, prices[6]);
    store!(schema, columns, weth_wsteth, prices[7]);
    store!(schema, columns, weth_reth, prices[8]);

    Ok(())
}
