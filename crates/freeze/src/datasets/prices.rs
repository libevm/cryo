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
const PRICE_QUERY_CALLDATA: &str = "608060405234801561001057600080fd5b50600061003673deb288f737066589598e9214e782fa5a8ed689e86111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006100509190611b6b565b9050600061007773986b5e1e1755e3c2440e960477f25201b0a8bbd46111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006100919190611b6b565b905060006402540be4006100be735f4ec3df9cbd43714fe2740f5e3616155c5b84196111b660201b60201c565b6100c89190611b9c565b905060006100ef73d10abbc76679a20055e167bb80a24ac851b370566111b660201b60201c565b6305f5e100836100ff9190611b9c565b6101099190611b6b565b90506000610130737bac85a8a13a4bcd8abb3eb7d6b4d632c5a576766111b660201b60201c565b6305f5e100846101409190611b9c565b61014a9190611b6b565b90506000858561017373773616e4d11a78f511299002da57a0a94577f1f46111b660201b60201c565b6ec097ce7bc90715b34b9f100000000061018d9190611b6b565b6101b073ee9f2375b4bdf6387aa8265dd4fb8f16512a1d466111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006101ca9190611b6b565b6101ed73dc530d9457755926550b59e8eccdae76241815576111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006102079190611b6b565b61022a7324551a8fb2a7211a25a17b1481f043a8a8adc7f26111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006102449190611b6b565b610267738e0b7e6062272b5ef4524250bfff8e5bd34977576111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006102819190611b6b565b6102a47379291a9d692df95334b1a0b3b4ae6bc606782f8c6111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006102be9190611b6b565b6040516020016102d5989796959493929190611bed565b60405160208183030381529060405290508061030a732de7e4a9488488e0058b95854cc2f7955b35dc9b6111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006103249190611b6b565b610347737c5d4f8345e66f68099581db340cd65b078c41f46111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006103619190611b6b565b610384731b39ee86ec5979ba5c322b826b3ecb8c799916996111b660201b60201c565b6ec097ce7bc90715b34b9f100000000061039e9190611b6b565b6103c1738a12be339b0cd1829b91adc01977caa5e9ac121e6111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006103db9190611b6b565b6103fe736df09e975c830ecae5bd4ed9d90f3a95a4f880126111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006104189190611b6b565b61043b73c1438aa3823a6ba0c159cfa8d98df5a994ba120b6111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006104559190611b6b565b61047873d6aa3d25116d8da79ea0246c4826eb951872e02e6111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006104929190611b6b565b6104b573e572cef69f43c2e488b33924af04bdace19079cf6111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006104cf9190611b6b565b6040516020016104e6989796959493929190611bed565b604051602081830303815290604052604051602001610506929190611cdc565b60405160208183030381529060405290508061053b7314d04fff8d21bd62987a5ce9ce543d2f1edf5d3e6111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006105559190611b6b565b6105787386392dc19c0b719886221c78ab11eb8cf5c528126111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006105929190611b6b565b6105b573536218f9e9eb48863970252233c8f271f554c2d06111b660201b60201c565b6ec097ce7bc90715b34b9f10000000006105cf9190611b6b565b6105f2734e844125952d32acdf339be976c98e22f6f318db6111b660201b60201c565b6ec097ce7bc90715b34b9f100000000061060c9190611b6b565b878760405160200161062396959493929190611d00565b604051602081830303815290604052604051602001610643929190611cdc565b604051602081830303815290604052905080610696734d5ef58aac27d99935e5b6b4a6778ff29205999173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6106d773811beed0119b4afce20d2583eb608c6f7af1954f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b61071873b6909b960dbbe7392d405429eb2b3649752b483873c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b610759738878df9e1a7c87dcbf6d3999d997f262c05d8c7073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b61079a737924a818013f39cf800f5589ff1f1f0def54f31f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6107db732e81ec0b8b4022fac83a21b2f2b4b8f5ed744d7073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6040516020016107f096959493929190611d00565b604051602081830303815290604052604051602001610810929190611cdc565b60405160208183030381529060405290508061086373742c15d71ea7444964bc39b0ed729b3729adc36173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6108a47305b0c1d8839ef3a989b33b6b63d3aa96cb7ec14273c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6108e57361eb53ee427ab4e007d78a9134aacb3101a2dc2373c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b610926734a86c01d67965f8cb3d0aaa2c655705e64097c3173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b610967736ada49aeccf6e556bb7a35ef0119cc8ca795294a73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6109a8732cc846fff0b08fb3bffad71f53a60b4b6e6d648273c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6109e973cbe856765eeec3fdc505ddebf9dc612da995e59373c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a764000061130a60201b60201c565b6040516020016109ff9796959493929190611d61565b604051602081830303815290604052604051602001610a1f929190611cdc565b604051602081830303815290604052905080670de0b6b3a764000064e8d4a51000610a7c738c1c499b1796d7f3c2521ac37186b52de024e58c73a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48620f424061130a60201b60201c565b610a869190611b9c565b87610a919190611b9c565b610a9b9190611b6b565b670de0b6b3a76400006402540be400610ae773110492b31c59716ac47337e616804e3e3adc0b4a732260fac5e5542a773aa44fbcfedf7c193bc2c5996305f5e10061130a60201b60201c565b610af19190611b9c565b89610afc9190611b9c565b610b069190611b6b565b604051602001610b17929190611dd0565b604051602081830303815290604052604051602001610b37929190611cdc565b604051602081830303815290604052905080610b8173151ccb92bc1ed5c6d0f9adb5cec4763ceb66ac7f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610bb973d8de6af55f618a7bc69835d55ddc6582220c36c073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610bf173e931b03260b2854e77e8da8378a1bc017b13cb9773c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610c2973d1d5a4c0ea98971894772dcd6d2f1dc71083c44e73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610c617392560c178ce069cc014138ed3c2f5221ba71f58a73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610c9973e936f0073549ad8b1fa53583600d629ba937516173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610cd17381fbbc40cf075fd7de6afce1bc72eda1bb0e13aa73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b604051602001610ce79796959493929190611d61565b604051602081830303815290604052604051602001610d07929190611cdc565b604051602081830303815290604052905080610d5173c4472dcd0e42ffccc1dbb0b9b3855688c22f3a0f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610d897399132b53ab44694eeb372e87bced3929e4ab845673c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610dc173465e56cd21ad47d4d4790f17de5e0458f20c371973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610df97324ee2c6b9597f035088cda8575e9d5e15a84b9df73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610e31738661ae7918c0115af9e3691662f605e9c550ddc973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610e69735b97b125cf8af96834f2d08c8f1291bd4772493973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610ea173e42318ea3b998e8355a3da364eb9d48ec725eb4573c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b604051602001610eb79796959493929190611d61565b604051602081830303815290604052604051602001610ed7929190611cdc565b604051602081830303815290604052905080610f2173e2c5d82523e0e767b83d78e2bfc6fcd74d1432ef73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610f597394981f69f7483af3ae218cbfe65233cc3c60d93a73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610f9173824a30f2984f9013f2c8d0a29c0a3cc5fd5c067373c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b610fc97314424eeecbff345b38187d0b8b749e56faa6853973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b61100173f56d08221b5942c428acc5de8f78489a97fc559973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b6110397311950d141ecb863f01007add7d1a342041227b5873c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b61107173510100d5143e011db24e2aa38abe85d73d5b217773c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261167960201b60201c565b6040516020016110879796959493929190611d61565b6040516020818303038152906040526040516020016110a7929190611cdc565b604051602081830303815290604052905080670de0b6b3a76400006110fa737270233ccae676e776a659affc35219e6fcfbb1073a0b86991c6218b36c1d19d4a2e9eb0ce3606eb4861167960201b60201c565b876111059190611b9c565b61110f9190611b6b565b670de0b6b3a76400006111507387d1b1a3675ff4ff6101926c1cce971cd2d513ef732260fac5e5542a773aa44fbcfedf7c193bc2c59961167960201b60201c565b8961115b9190611b9c565b6111659190611b6b565b604051602001611176929190611dd0565b604051602081830303815290604052604051602001611196929190611cdc565b604051602081830303815290604052905060008190506020810180590381f35b60006111c78261199560201b60201c565b6111db57670de0b6b3a76400009050611305565b6000808373ffffffffffffffffffffffffffffffffffffffff166350d25bcd60e01b604051602401604051602081830303815290604052907bffffffffffffffffffffffffffffffffffffffffffffffffffffffff19166020820180517bffffffffffffffffffffffffffffffffffffffffffffffffffffffff838183161783525050505060405161126d9190611df9565b6000604051808303816000865af19150503d80600081146112aa576040519150601f19603f3d011682016040523d82523d6000602084013e6112af565b606091505b5091509150816112cb57670de0b6b3a764000092505050611305565b6000818060200190518101906112e19190611e4b565b9050600081036112fe57670de0b6b3a76400009350505050611305565b8093505050505b919050565b600061131b8461199560201b60201c565b61132f57670de0b6b3a76400009050611672565b600084905060008173ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa158015611381573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906113a59190611ed6565b905060008273ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa1580156113f4573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906114189190611ed6565b90506000808473ffffffffffffffffffffffffffffffffffffffff16630902f1ac6040518163ffffffff1660e01b8152600401606060405180830381865afa158015611468573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061148c9190611f85565b506dffffffffffffffffffffffffffff1691506dffffffffffffffffffffffffffff16915060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa1580156114fe573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906115229190612004565b905060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa158015611571573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906115959190612004565b90508473ffffffffffffffffffffffffffffffffffffffff168a73ffffffffffffffffffffffffffffffffffffffff16036116205780600a6115d79190612164565b846115e29190611b9c565b935081600a6115f19190612164565b83858b6115fe9190611b9c565b6116089190611b6b565b6116129190611b6b565b975050505050505050611672565b81600a61162d9190612164565b836116389190611b9c565b925080600a6116479190612164565b84848b6116549190611b9c565b61165e9190611b6b565b6116689190611b6b565b9750505050505050505b9392505050565b600061168a8361199560201b60201c565b61169e57670de0b6b3a7640000905061198f565b600083905060008173ffffffffffffffffffffffffffffffffffffffff16633850c7bd6040518163ffffffff1660e01b815260040160e060405180830381865afa1580156116f0573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061171491906122bf565b505050505050905060008273ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa158015611769573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061178d9190611ed6565b905060008373ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa1580156117dc573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906118009190611ed6565b905060008273ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa15801561184f573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906118739190612004565b905060008273ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa1580156118c2573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906118e69190612004565b905060006118fa86846119a860201b60201c565b90508873ffffffffffffffffffffffffffffffffffffffff168573ffffffffffffffffffffffffffffffffffffffff16036119615781601261193c9190612361565b600a6119489190612164565b816119539190611b9c565b97505050505050505061198f565b8082601261196f9190612395565b600a61197b9190612164565b6119859190611b6b565b9750505050505050505b92915050565b600080823b905060008111915050919050565b6000808373ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff166119e39190611b9c565b9050600083600a6119f491906123c9565b9050611a2082827801000000000000000000000000000000000000000000000000611a2a60201b60201c565b9250505092915050565b6000806000801985870985870292508281108382030391505060008103611a645760008411611a5857600080fd5b83820492505050611afc565b808411611a7057600080fd5b600084868809905082811182039150808303925060008586600003169050808604955080840493506001818260000304019050808302841793506000600287600302189050808702600203810290508087026002038102905080870260020381029050808702600203810290508087026002038102905080870260020381029050808502955050505050505b9392505050565b6000819050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b6000611b7682611b03565b9150611b8183611b03565b925082611b9157611b90611b0d565b5b828204905092915050565b6000611ba782611b03565b9150611bb283611b03565b9250828202611bc081611b03565b91508282048414831517611bd757611bd6611b3c565b5b5092915050565b611be781611b03565b82525050565b600061010082019050611c03600083018b611bde565b611c10602083018a611bde565b611c1d6040830189611bde565b611c2a6060830188611bde565b611c376080830187611bde565b611c4460a0830186611bde565b611c5160c0830185611bde565b611c5e60e0830184611bde565b9998505050505050505050565b600081519050919050565b600081905092915050565b60005b83811015611c9f578082015181840152602081019050611c84565b60008484015250505050565b6000611cb682611c6b565b611cc08185611c76565b9350611cd0818560208601611c81565b80840191505092915050565b6000611ce88285611cab565b9150611cf48284611cab565b91508190509392505050565b600060c082019050611d156000830189611bde565b611d226020830188611bde565b611d2f6040830187611bde565b611d3c6060830186611bde565b611d496080830185611bde565b611d5660a0830184611bde565b979650505050505050565b600060e082019050611d76600083018a611bde565b611d836020830189611bde565b611d906040830188611bde565b611d9d6060830187611bde565b611daa6080830186611bde565b611db760a0830185611bde565b611dc460c0830184611bde565b98975050505050505050565b6000604082019050611de56000830185611bde565b611df26020830184611bde565b9392505050565b6000611e058284611cab565b915081905092915050565b600080fd5b6000819050919050565b611e2881611e15565b8114611e3357600080fd5b50565b600081519050611e4581611e1f565b92915050565b600060208284031215611e6157611e60611e10565b5b6000611e6f84828501611e36565b91505092915050565b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b6000611ea382611e78565b9050919050565b611eb381611e98565b8114611ebe57600080fd5b50565b600081519050611ed081611eaa565b92915050565b600060208284031215611eec57611eeb611e10565b5b6000611efa84828501611ec1565b91505092915050565b60006dffffffffffffffffffffffffffff82169050919050565b611f2681611f03565b8114611f3157600080fd5b50565b600081519050611f4381611f1d565b92915050565b600063ffffffff82169050919050565b611f6281611f49565b8114611f6d57600080fd5b50565b600081519050611f7f81611f59565b92915050565b600080600060608486031215611f9e57611f9d611e10565b5b6000611fac86828701611f34565b9350506020611fbd86828701611f34565b9250506040611fce86828701611f70565b9150509250925092565b611fe181611b03565b8114611fec57600080fd5b50565b600081519050611ffe81611fd8565b92915050565b60006020828403121561201a57612019611e10565b5b600061202884828501611fef565b91505092915050565b60008160011c9050919050565b6000808291508390505b60018511156120885780860481111561206457612063611b3c565b5b60018516156120735780820291505b808102905061208185612031565b9450612048565b94509492505050565b6000826120a1576001905061215d565b816120af576000905061215d565b81600181146120c557600281146120cf576120fe565b600191505061215d565b60ff8411156120e1576120e0611b3c565b5b8360020a9150848211156120f8576120f7611b3c565b5b5061215d565b5060208310610133831016604e8410600b84101617156121335782820a90508381111561212e5761212d611b3c565b5b61215d565b612140848484600161203e565b9250905081840481111561215757612156611b3c565b5b81810290505b9392505050565b600061216f82611b03565b915061217a83611b03565b92506121a77fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8484612091565b905092915050565b6121b881611e78565b81146121c357600080fd5b50565b6000815190506121d5816121af565b92915050565b60008160020b9050919050565b6121f1816121db565b81146121fc57600080fd5b50565b60008151905061220e816121e8565b92915050565b600061ffff82169050919050565b61222b81612214565b811461223657600080fd5b50565b60008151905061224881612222565b92915050565b600060ff82169050919050565b6122648161224e565b811461226f57600080fd5b50565b6000815190506122818161225b565b92915050565b60008115159050919050565b61229c81612287565b81146122a757600080fd5b50565b6000815190506122b981612293565b92915050565b600080600080600080600060e0888a0312156122de576122dd611e10565b5b60006122ec8a828b016121c6565b97505060206122fd8a828b016121ff565b965050604061230e8a828b01612239565b955050606061231f8a828b01612239565b94505060806123308a828b01612239565b93505060a06123418a828b01612272565b92505060c06123528a828b016122aa565b91505092959891949750929550565b600061236c82611b03565b915061237783611b03565b925082820390508181111561238f5761238e611b3c565b5b92915050565b60006123a082611b03565b91506123ab83611b03565b92508282019050808211156123c3576123c2611b3c565b5b92915050565b60006123d482611b03565b91506123df8361224e565b925061240c7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff8484612091565b90509291505056fe";

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
            ("eth_ldo", ColumnType::Float64),
            ("eth_ape", ColumnType::Float64),
            ("eth_matic", ColumnType::Float64),
            // ---
            ("eth_dpi", ColumnType::Float64),
            ("eth_shiba", ColumnType::Float64),
            ("eth_bat", ColumnType::Float64),
            ("eth_lrc", ColumnType::Float64),
            ("eth_lon", ColumnType::Float64),
            ("eth_grt", ColumnType::Float64),
            ("eth_omg", ColumnType::Float64),
            ("eth_fun", ColumnType::Float64),
            ("eth_fxs", ColumnType::Float64),
            ("eth_syn", ColumnType::Float64),
            ("eth_woo", ColumnType::Float64),
            ("eth_hpsoi", ColumnType::Float64),
            ("eth_ladys", ColumnType::Float64),
            ("eth_rad", ColumnType::Float64),
            ("eth_badger", ColumnType::Float64),
            ("eth_eth2xfli", ColumnType::Float64),
            ("eth_dydx", ColumnType::Float64),
            ("eth_1inch", ColumnType::Float64),
            ("eth_lqty", ColumnType::Float64),
            ("eth_ens", ColumnType::Float64),
            ("eth_render", ColumnType::Float64),
            ("eth_imx", ColumnType::Float64),
            ("eth_wld", ColumnType::Float64),
            ("eth_agix", ColumnType::Float64),
            ("eth_gala", ColumnType::Float64),
            ("eth_qnt", ColumnType::Float64),
            ("eth_mana", ColumnType::Float64),
            ("eth_sand", ColumnType::Float64),
            ("eth_rpl", ColumnType::Float64),
            ("eth_tribe", ColumnType::Float64),
            ("eth_ribbon", ColumnType::Float64),
            ("eth_blur", ColumnType::Float64),
            ("eth_zrx", ColumnType::Float64),
            ("eth_gno", ColumnType::Float64),
            ("eth_pepe", ColumnType::Float64),
            ("eth_rlb", ColumnType::Float64),
            ("eth_wcfg", ColumnType::Float64),
            ("eth_btc2xfli", ColumnType::Float64),
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
            "eth_ldo",
            "eth_ape",
            "eth_matic",
            "eth_dpi",
            "eth_shiba",
            "eth_bat",
            "eth_lrc",
            "eth_lon",
            "eth_grt",
            "eth_omg",
            "eth_fun",
            "eth_fxs",
            "eth_syn",
            "eth_woo",
            "eth_hpsoi",
            "eth_ladys",
            "eth_rad",
            "eth_badger",
            "eth_eth2xfli",
            "eth_dydx",
            "eth_1inch",
            "eth_lqty",
            "eth_ens",
            "eth_render",
            "eth_imx",
            "eth_wld",
            "eth_agix",
            "eth_gala",
            "eth_qnt",
            "eth_mana",
            "eth_sand",
            "eth_rpl",
            "eth_tribe",
            "eth_ribbon",
            "eth_blur",
            "eth_zrx",
            "eth_gno",
            "eth_pepe",
            "eth_rlb",
            "eth_wcfg",
            "eth_btc2xfli",
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
            "eth_ldo".to_string(),
            "eth_ape".to_string(),
            "eth_matic".to_string(),
            "eth_dpi".to_string(),
            "eth_shiba".to_string(),
            "eth_bat".to_string(),
            "eth_lrc".to_string(),
            "eth_lon".to_string(),
            "eth_grt".to_string(),
            "eth_omg".to_string(),
            "eth_fun".to_string(),
            "eth_fxs".to_string(),
            "eth_syn".to_string(),
            "eth_woo".to_string(),
            "eth_hpsoi".to_string(),
            "eth_ladys".to_string(),
            "eth_rad".to_string(),
            "eth_badger".to_string(),
            "eth_eth2xfli".to_string(),
            "eth_dydx".to_string(),
            "eth_1inch".to_string(),
            "eth_lqty".to_string(),
            "eth_ens".to_string(),
            "eth_render".to_string(),
            "eth_imx".to_string(),
            "eth_wld".to_string(),
            "eth_agix".to_string(),
            "eth_gala".to_string(),
            "eth_qnt".to_string(),
            "eth_mana".to_string(),
            "eth_sand".to_string(),
            "eth_rpl".to_string(),
            "eth_tribe".to_string(),
            "eth_ribbon".to_string(),
            "eth_blur".to_string(),
            "eth_zrx".to_string(),
            "eth_gno".to_string(),
            "eth_pepe".to_string(),
            "eth_rlb".to_string(),
            "eth_wcfg".to_string(),
            "eth_btc2xfli".to_string(),
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
                    let eth_ldo = format_ether(price_res[19]).parse::<f64>().unwrap();
                    let eth_ape = format_ether(price_res[20]).parse::<f64>().unwrap();
                    let eth_matic = format_ether(price_res[21]).parse::<f64>().unwrap();
                    let eth_dpi = format_ether(price_res[22]).parse::<f64>().unwrap();
                    let eth_shiba = format_ether(price_res[23]).parse::<f64>().unwrap();
                    let eth_bat = format_ether(price_res[24]).parse::<f64>().unwrap();
                    let eth_lrc = format_ether(price_res[25]).parse::<f64>().unwrap();
                    let eth_lon = format_ether(price_res[26]).parse::<f64>().unwrap();
                    let eth_grt = format_ether(price_res[27]).parse::<f64>().unwrap();
                    let eth_omg = format_ether(price_res[28]).parse::<f64>().unwrap();
                    let eth_fun = format_ether(price_res[29]).parse::<f64>().unwrap();
                    let eth_fxs = format_ether(price_res[30]).parse::<f64>().unwrap();
                    let eth_syn = format_ether(price_res[31]).parse::<f64>().unwrap();
                    let eth_woo = format_ether(price_res[32]).parse::<f64>().unwrap();
                    let eth_hpsoi = format_ether(price_res[33]).parse::<f64>().unwrap();
                    let eth_ladys = format_ether(price_res[34]).parse::<f64>().unwrap();
                    let eth_rad = format_ether(price_res[35]).parse::<f64>().unwrap();
                    let eth_badger = format_ether(price_res[36]).parse::<f64>().unwrap();
                    let eth_eth2xfli = format_ether(price_res[37]).parse::<f64>().unwrap();
                    let eth_dydx = format_ether(price_res[38]).parse::<f64>().unwrap();
                    let eth_1inch = format_ether(price_res[39]).parse::<f64>().unwrap();
                    let eth_lqty = format_ether(price_res[40]).parse::<f64>().unwrap();
                    let eth_ens = format_ether(price_res[41]).parse::<f64>().unwrap();
                    let eth_render = format_ether(price_res[42]).parse::<f64>().unwrap();
                    let eth_imx = format_ether(price_res[43]).parse::<f64>().unwrap();
                    let eth_wld = format_ether(price_res[44]).parse::<f64>().unwrap();
                    let eth_agix = format_ether(price_res[45]).parse::<f64>().unwrap();
                    let eth_gala = format_ether(price_res[46]).parse::<f64>().unwrap();
                    let eth_qnt = format_ether(price_res[47]).parse::<f64>().unwrap();
                    let eth_mana = format_ether(price_res[48]).parse::<f64>().unwrap();
                    let eth_sand = format_ether(price_res[49]).parse::<f64>().unwrap();
                    let eth_rpl = format_ether(price_res[50]).parse::<f64>().unwrap();
                    let eth_tribe = format_ether(price_res[51]).parse::<f64>().unwrap();
                    let eth_ribbon = format_ether(price_res[52]).parse::<f64>().unwrap();
                    let eth_blur = format_ether(price_res[53]).parse::<f64>().unwrap();
                    let eth_zrx = format_ether(price_res[54]).parse::<f64>().unwrap();
                    let eth_gno = format_ether(price_res[55]).parse::<f64>().unwrap();
                    let eth_pepe = format_ether(price_res[56]).parse::<f64>().unwrap();
                    let eth_rlb = format_ether(price_res[57]).parse::<f64>().unwrap();
                    let eth_wcfg = format_ether(price_res[58]).parse::<f64>().unwrap();
                    let eth_btc2xfli = format_ether(price_res[59]).parse::<f64>().unwrap();
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
                        eth_ldo,
                        eth_ape,
                        eth_matic,
                        eth_dpi,
                        eth_shiba,
                        eth_bat,
                        eth_lrc,
                        eth_lon,
                        eth_grt,
                        eth_omg,
                        eth_fun,
                        eth_fxs,
                        eth_syn,
                        eth_woo,
                        eth_hpsoi,
                        eth_ladys,
                        eth_rad,
                        eth_badger,
                        eth_eth2xfli,
                        eth_dydx,
                        eth_1inch,
                        eth_lqty,
                        eth_ens,
                        eth_render,
                        eth_imx,
                        eth_wld,
                        eth_agix,
                        eth_gala,
                        eth_qnt,
                        eth_mana,
                        eth_sand,
                        eth_rpl,
                        eth_tribe,
                        eth_ribbon,
                        eth_blur,
                        eth_zrx,
                        eth_gno,
                        eth_pepe,
                        eth_rlb,
                        eth_wcfg,
                        eth_btc2xfli,
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

pub(crate) struct BlockWithPrices {
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
    eth_ldo: f64,
    eth_ape: f64,
    eth_matic: f64,
    eth_dpi: f64,
    eth_shiba: f64,
    eth_bat: f64,
    eth_lrc: f64,
    eth_lon: f64,
    eth_grt: f64,
    eth_omg: f64,
    eth_fun: f64,
    eth_fxs: f64,
    eth_syn: f64,
    eth_woo: f64,
    eth_hpsoi: f64,
    eth_ladys: f64,
    eth_rad: f64,
    eth_badger: f64,
    eth_eth2xfli: f64,
    eth_dydx: f64,
    eth_1inch: f64,
    eth_lqty: f64,
    eth_ens: f64,
    eth_render: f64,
    eth_imx: f64,
    eth_wld: f64,
    eth_agix: f64,
    eth_gala: f64,
    eth_qnt: f64,
    eth_mana: f64,
    eth_sand: f64,
    eth_rpl: f64,
    eth_tribe: f64,
    eth_ribbon: f64,
    eth_blur: f64,
    eth_zrx: f64,
    eth_gno: f64,
    eth_pepe: f64,
    eth_rlb: f64,
    eth_wcfg: f64,
    eth_btc2xfli: f64,
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
    eth_ldo: Vec<f64>,
    eth_ape: Vec<f64>,
    eth_matic: Vec<f64>,
    eth_dpi: Vec<f64>,
    eth_shiba: Vec<f64>,
    eth_bat: Vec<f64>,
    eth_lrc: Vec<f64>,
    eth_lon: Vec<f64>,
    eth_grt: Vec<f64>,
    eth_omg: Vec<f64>,
    eth_fun: Vec<f64>,
    eth_fxs: Vec<f64>,
    eth_syn: Vec<f64>,
    eth_woo: Vec<f64>,
    eth_hpsoi: Vec<f64>,
    eth_ladys: Vec<f64>,
    eth_rad: Vec<f64>,
    eth_badger: Vec<f64>,
    eth_eth2xfli: Vec<f64>,
    eth_dydx: Vec<f64>,
    eth_1inch: Vec<f64>,
    eth_lqty: Vec<f64>,
    eth_ens: Vec<f64>,
    eth_render: Vec<f64>,
    eth_imx: Vec<f64>,
    eth_wld: Vec<f64>,
    eth_agix: Vec<f64>,
    eth_gala: Vec<f64>,
    eth_qnt: Vec<f64>,
    eth_mana: Vec<f64>,
    eth_sand: Vec<f64>,
    eth_rpl: Vec<f64>,
    eth_tribe: Vec<f64>,
    eth_ribbon: Vec<f64>,
    eth_blur: Vec<f64>,
    eth_zrx: Vec<f64>,
    eth_gno: Vec<f64>,
    eth_pepe: Vec<f64>,
    eth_rlb: Vec<f64>,
    eth_wcfg: Vec<f64>,
    eth_btc2xfli: Vec<f64>,
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
        if schema.has_column("eth_ldo") {
            self.eth_ldo.push(prices.eth_ldo);
        }
        if schema.has_column("eth_ape") {
            self.eth_ape.push(prices.eth_ape);
        }
        if schema.has_column("eth_matic") {
            self.eth_matic.push(prices.eth_matic);
        }
        if schema.has_column("eth_dpi") {
            self.eth_dpi.push(prices.eth_dpi);
        }
        if schema.has_column("eth_shiba") {
            self.eth_shiba.push(prices.eth_shiba);
        }
        if schema.has_column("eth_bat") {
            self.eth_bat.push(prices.eth_bat);
        }
        if schema.has_column("eth_lrc") {
            self.eth_lrc.push(prices.eth_lrc);
        }
        if schema.has_column("eth_lon") {
            self.eth_lon.push(prices.eth_lon);
        }
        if schema.has_column("eth_grt") {
            self.eth_grt.push(prices.eth_grt);
        }
        if schema.has_column("eth_omg") {
            self.eth_omg.push(prices.eth_omg);
        }
        if schema.has_column("eth_fun") {
            self.eth_fun.push(prices.eth_fun);
        }
        if schema.has_column("eth_fxs") {
            self.eth_fxs.push(prices.eth_fxs);
        }
        if schema.has_column("eth_syn") {
            self.eth_syn.push(prices.eth_syn);
        }
        if schema.has_column("eth_woo") {
            self.eth_woo.push(prices.eth_woo);
        }
        if schema.has_column("eth_hpsoi") {
            self.eth_hpsoi.push(prices.eth_hpsoi);
        }
        if schema.has_column("eth_ladys") {
            self.eth_ladys.push(prices.eth_ladys);
        }
        if schema.has_column("eth_rad") {
            self.eth_rad.push(prices.eth_rad);
        }
        if schema.has_column("eth_badger") {
            self.eth_badger.push(prices.eth_badger);
        }
        if schema.has_column("eth_eth2xfli") {
            self.eth_eth2xfli.push(prices.eth_eth2xfli);
        }
        if schema.has_column("eth_dydx") {
            self.eth_dydx.push(prices.eth_dydx);
        }
        if schema.has_column("eth_1inch") {
            self.eth_1inch.push(prices.eth_1inch);
        }
        if schema.has_column("eth_lqty") {
            self.eth_lqty.push(prices.eth_lqty);
        }
        if schema.has_column("eth_ens") {
            self.eth_ens.push(prices.eth_ens);
        }
        if schema.has_column("eth_render") {
            self.eth_render.push(prices.eth_render);
        }
        if schema.has_column("eth_imx") {
            self.eth_imx.push(prices.eth_imx);
        }
        if schema.has_column("eth_wld") {
            self.eth_wld.push(prices.eth_wld);
        }
        if schema.has_column("eth_agix") {
            self.eth_agix.push(prices.eth_agix);
        }
        if schema.has_column("eth_gala") {
            self.eth_gala.push(prices.eth_gala);
        }
        if schema.has_column("eth_qnt") {
            self.eth_qnt.push(prices.eth_qnt);
        }
        if schema.has_column("eth_mana") {
            self.eth_mana.push(prices.eth_mana);
        }
        if schema.has_column("eth_sand") {
            self.eth_sand.push(prices.eth_sand);
        }
        if schema.has_column("eth_rpl") {
            self.eth_rpl.push(prices.eth_rpl);
        }
        if schema.has_column("eth_tribe") {
            self.eth_tribe.push(prices.eth_tribe);
        }
        if schema.has_column("eth_ribbon") {
            self.eth_ribbon.push(prices.eth_ribbon);
        }
        if schema.has_column("eth_blur") {
            self.eth_blur.push(prices.eth_blur);
        }
        if schema.has_column("eth_zrx") {
            self.eth_zrx.push(prices.eth_zrx);
        }
        if schema.has_column("eth_gno") {
            self.eth_gno.push(prices.eth_gno);
        }
        if schema.has_column("eth_pepe") {
            self.eth_pepe.push(prices.eth_pepe);
        }
        if schema.has_column("eth_rlb") {
            self.eth_rlb.push(prices.eth_rlb);
        }
        if schema.has_column("eth_wcfg") {
            self.eth_wcfg.push(prices.eth_wcfg);
        }
        if schema.has_column("eth_btc2xfli") {
            self.eth_btc2xfli.push(prices.eth_btc2xfli);
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
        with_series!(cols, "eth_ldo", self.eth_ldo, schema);
        with_series!(cols, "eth_ape", self.eth_ape, schema);
        with_series!(cols, "eth_matic", self.eth_matic, schema);
        with_series!(cols, "eth_dpi", self.eth_dpi, schema);
        with_series!(cols, "eth_shiba", self.eth_shiba, schema);
        with_series!(cols, "eth_bat", self.eth_bat, schema);
        with_series!(cols, "eth_lrc", self.eth_lrc, schema);
        with_series!(cols, "eth_lon", self.eth_lon, schema);
        with_series!(cols, "eth_grt", self.eth_grt, schema);
        with_series!(cols, "eth_omg", self.eth_omg, schema);
        with_series!(cols, "eth_fun", self.eth_fun, schema);
        with_series!(cols, "eth_fxs", self.eth_fxs, schema);
        with_series!(cols, "eth_syn", self.eth_syn, schema);
        with_series!(cols, "eth_woo", self.eth_woo, schema);
        with_series!(cols, "eth_hpsoi", self.eth_hpsoi, schema);
        with_series!(cols, "eth_ladys", self.eth_ladys, schema);
        with_series!(cols, "eth_rad", self.eth_rad, schema);
        with_series!(cols, "eth_badger", self.eth_badger, schema);
        with_series!(cols, "eth_eth2xfli", self.eth_eth2xfli, schema);
        with_series!(cols, "eth_dydx", self.eth_dydx, schema);
        with_series!(cols, "eth_1inch", self.eth_1inch, schema);
        with_series!(cols, "eth_lqty", self.eth_lqty, schema);
        with_series!(cols, "eth_ens", self.eth_ens, schema);
        with_series!(cols, "eth_render", self.eth_render, schema);
        with_series!(cols, "eth_imx", self.eth_imx, schema);
        with_series!(cols, "eth_wld", self.eth_wld, schema);
        with_series!(cols, "eth_agix", self.eth_agix, schema);
        with_series!(cols, "eth_gala", self.eth_gala, schema);
        with_series!(cols, "eth_qnt", self.eth_qnt, schema);
        with_series!(cols, "eth_mana", self.eth_mana, schema);
        with_series!(cols, "eth_sand", self.eth_sand, schema);
        with_series!(cols, "eth_rpl", self.eth_rpl, schema);
        with_series!(cols, "eth_tribe", self.eth_tribe, schema);
        with_series!(cols, "eth_ribbon", self.eth_ribbon, schema);
        with_series!(cols, "eth_blur", self.eth_blur, schema);
        with_series!(cols, "eth_zrx", self.eth_zrx, schema);
        with_series!(cols, "eth_gno", self.eth_gno, schema);
        with_series!(cols, "eth_pepe", self.eth_pepe, schema);
        with_series!(cols, "eth_rlb", self.eth_rlb, schema);
        with_series!(cols, "eth_wcfg", self.eth_wcfg, schema);
        with_series!(cols, "eth_btc2xfli", self.eth_btc2xfli, schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
