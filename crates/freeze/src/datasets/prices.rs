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
const PRICE_QUERY_CALLDATA: &str = "60a060405262fdbdd0431161002857736f41040b9e098c2ac4b88e27b50d4e9ab486781b61003e565b73824a30f2984f9013f2c8d0a29c0a3cc5fd5c06735b73ffffffffffffffffffffffffffffffffffffffff1660809073ffffffffffffffffffffffffffffffffffffffff1681525034801561007c57600080fd5b5060006100a273deb288f737066589598e9214e782fa5a8ed689e861129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006100bc9190611cbf565b905060006100e373986b5e1e1755e3c2440e960477f25201b0a8bbd461129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006100fd9190611cbf565b905060006402540be40061012a735f4ec3df9cbd43714fe2740f5e3616155c5b841961129d60201b60201c565b6101349190611cf0565b9050600061015b73d10abbc76679a20055e167bb80a24ac851b3705661129d60201b60201c565b6305f5e1008361016b9190611cf0565b6101759190611cbf565b9050600061019c737bac85a8a13a4bcd8abb3eb7d6b4d632c5a5767661129d60201b60201c565b6305f5e100846101ac9190611cf0565b6101b69190611cbf565b9050600085856101df73773616e4d11a78f511299002da57a0a94577f1f461129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006101f99190611cbf565b61021c73ee9f2375b4bdf6387aa8265dd4fb8f16512a1d4661129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006102369190611cbf565b61025973dc530d9457755926550b59e8eccdae762418155761129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006102739190611cbf565b6102967324551a8fb2a7211a25a17b1481f043a8a8adc7f261129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006102b09190611cbf565b6102d3738e0b7e6062272b5ef4524250bfff8e5bd349775761129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006102ed9190611cbf565b6103107379291a9d692df95334b1a0b3b4ae6bc606782f8c61129d60201b60201c565b6ec097ce7bc90715b34b9f100000000061032a9190611cbf565b604051602001610341989796959493929190611d41565b604051602081830303815290604052905080610376732de7e4a9488488e0058b95854cc2f7955b35dc9b61129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006103909190611cbf565b6103b3737c5d4f8345e66f68099581db340cd65b078c41f461129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006103cd9190611cbf565b6103f0731b39ee86ec5979ba5c322b826b3ecb8c7999169961129d60201b60201c565b6ec097ce7bc90715b34b9f100000000061040a9190611cbf565b61042d738a12be339b0cd1829b91adc01977caa5e9ac121e61129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006104479190611cbf565b61046a736df09e975c830ecae5bd4ed9d90f3a95a4f8801261129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006104849190611cbf565b6104a773c1438aa3823a6ba0c159cfa8d98df5a994ba120b61129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006104c19190611cbf565b6104e473d6aa3d25116d8da79ea0246c4826eb951872e02e61129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006104fe9190611cbf565b61052173e572cef69f43c2e488b33924af04bdace19079cf61129d60201b60201c565b6ec097ce7bc90715b34b9f100000000061053b9190611cbf565b604051602001610552989796959493929190611d41565b604051602081830303815290604052604051602001610572929190611e30565b6040516020818303038152906040529050806105a77314d04fff8d21bd62987a5ce9ce543d2f1edf5d3e61129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006105c19190611cbf565b6105e47386392dc19c0b719886221c78ab11eb8cf5c5281261129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006105fe9190611cbf565b61062173536218f9e9eb48863970252233c8f271f554c2d061129d60201b60201c565b6ec097ce7bc90715b34b9f100000000061063b9190611cbf565b61065e734e844125952d32acdf339be976c98e22f6f318db61129d60201b60201c565b6ec097ce7bc90715b34b9f10000000006106789190611cbf565b878760405160200161068f96959493929190611e54565b6040516020818303038152906040526040516020016106af929190611e30565b604051602081830303815290604052905080610702734d5ef58aac27d99935e5b6b4a6778ff29205999173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b61074373811beed0119b4afce20d2583eb608c6f7af1954f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b61078473b6909b960dbbe7392d405429eb2b3649752b483873c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b6107c5738878df9e1a7c87dcbf6d3999d997f262c05d8c7073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b610806737924a818013f39cf800f5589ff1f1f0def54f31f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b610847732e81ec0b8b4022fac83a21b2f2b4b8f5ed744d7073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b60405160200161085c96959493929190611e54565b60405160208183030381529060405260405160200161087c929190611e30565b6040516020818303038152906040529050806108cf73742c15d71ea7444964bc39b0ed729b3729adc36173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b6109107305b0c1d8839ef3a989b33b6b63d3aa96cb7ec14273c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b6109517361eb53ee427ab4e007d78a9134aacb3101a2dc2373c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b610992734a86c01d67965f8cb3d0aaa2c655705e64097c3173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b6109d3736ada49aeccf6e556bb7a35ef0119cc8ca795294a73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b610a14732cc846fff0b08fb3bffad71f53a60b4b6e6d648273c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b610a5573cbe856765eeec3fdc505ddebf9dc612da995e59373c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b604051602001610a6b9796959493929190611eb5565b604051602081830303815290604052604051602001610a8b929190611e30565b604051602081830303815290604052905080670de0b6b3a764000064e8d4a51000610ae8738c1c499b1796d7f3c2521ac37186b52de024e58c73a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48620f42406113dc60201b60201c565b610af29190611cf0565b87610afd9190611cf0565b610b079190611cbf565b670de0b6b3a76400006402540be400610b5373110492b31c59716ac47337e616804e3e3adc0b4a732260fac5e5542a773aa44fbcfedf7c193bc2c5996305f5e1006113dc60201b60201c565b610b5d9190611cf0565b89610b689190611cf0565b610b729190611cbf565b604051602001610b83929190611f24565b604051602081830303815290604052604051602001610ba3929190611e30565b604051602081830303815290604052905080610bed73151ccb92bc1ed5c6d0f9adb5cec4763ceb66ac7f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610c2573d8de6af55f618a7bc69835d55ddc6582220c36c073c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610c5d73e931b03260b2854e77e8da8378a1bc017b13cb9773c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610c9573d1d5a4c0ea98971894772dcd6d2f1dc71083c44e73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610ccd7392560c178ce069cc014138ed3c2f5221ba71f58a73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610d0573e936f0073549ad8b1fa53583600d629ba937516173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610d3d7381fbbc40cf075fd7de6afce1bc72eda1bb0e13aa73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b604051602001610d539796959493929190611eb5565b604051602081830303815290604052604051602001610d73929190611e30565b604051602081830303815290604052905080610dbd73c4472dcd0e42ffccc1dbb0b9b3855688c22f3a0f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610df57399132b53ab44694eeb372e87bced3929e4ab845673c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610e2d73465e56cd21ad47d4d4790f17de5e0458f20c371973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610e657324ee2c6b9597f035088cda8575e9d5e15a84b9df73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610e9d738661ae7918c0115af9e3691662f605e9c550ddc973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610ed5735b97b125cf8af96834f2d08c8f1291bd4772493973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610f0d73e42318ea3b998e8355a3da364eb9d48ec725eb4573c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b604051602001610f239796959493929190611eb5565b604051602081830303815290604052604051602001610f43929190611e30565b604051602081830303815290604052905080610f8d73e2c5d82523e0e767b83d78e2bfc6fcd74d1432ef73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610fc57394981f69f7483af3ae218cbfe65233cc3c60d93a73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b610feb60805173c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b6110237314424eeecbff345b38187d0b8b749e56faa6853973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b61105b73f56d08221b5942c428acc5de8f78489a97fc559973c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b61109c73a43fe16908251ee70ef74718545e4fe6c5ccec9f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b6110d473510100d5143e011db24e2aa38abe85d73d5b217773c02aaa39b223fe8d0a0e5c4f27ead9083c756cc261174460201b60201c565b6040516020016110ea9796959493929190611eb5565b60405160208183030381529060405260405160200161110a929190611e30565b604051602081830303815290604052905080670de0b6b3a764000061115d737270233ccae676e776a659affc35219e6fcfbb1073a0b86991c6218b36c1d19d4a2e9eb0ce3606eb4861174460201b60201c565b876111689190611cf0565b6111729190611cbf565b670de0b6b3a76400006111b37387d1b1a3675ff4ff6101926c1cce971cd2d513ef732260fac5e5542a773aa44fbcfedf7c193bc2c59961174460201b60201c565b896111be9190611cf0565b6111c89190611cbf565b61120973c3f279090a47e80990fe3a9c30d24cb117ef91a873c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b61124a733fd4cf9303c4bc9e13772618828712c8eac7dd2f73c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2670de0b6b3a76400006113dc60201b60201c565b60405160200161125d9493929190611f4d565b60405160208183030381529060405260405160200161127d929190611e30565b604051602081830303815290604052905060008190506020810180590381f35b60006112ae82611ae960201b60201c565b6112bb57600190506113d7565b6000808373ffffffffffffffffffffffffffffffffffffffff166350d25bcd60e01b604051602401604051602081830303815290604052907bffffffffffffffffffffffffffffffffffffffffffffffffffffffff19166020820180517bffffffffffffffffffffffffffffffffffffffffffffffffffffffff838183161783525050505060405161134d9190611f92565b6000604051808303816000865af19150503d806000811461138a576040519150601f19603f3d011682016040523d82523d6000602084013e61138f565b606091505b5091509150816113a4576001925050506113d7565b6000818060200190518101906113ba9190611fe4565b9050600081036113d057600193505050506113d7565b8093505050505b919050565b60006113ed84611ae960201b60201c565b6113fa576001905061173d565b600084905060008173ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa15801561144c573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190611470919061206f565b905060008273ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa1580156114bf573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906114e3919061206f565b90506000808473ffffffffffffffffffffffffffffffffffffffff16630902f1ac6040518163ffffffff1660e01b8152600401606060405180830381865afa158015611533573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190611557919061211e565b506dffffffffffffffffffffffffffff1691506dffffffffffffffffffffffffffff16915060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa1580156115c9573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906115ed919061219d565b905060008473ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa15801561163c573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190611660919061219d565b90508473ffffffffffffffffffffffffffffffffffffffff168a73ffffffffffffffffffffffffffffffffffffffff16036116eb5780600a6116a291906122fd565b846116ad9190611cf0565b935081600a6116bc91906122fd565b83858b6116c99190611cf0565b6116d39190611cbf565b6116dd9190611cbf565b97505050505050505061173d565b81600a6116f891906122fd565b836117039190611cf0565b925080600a61171291906122fd565b84848b61171f9190611cf0565b6117299190611cbf565b6117339190611cbf565b9750505050505050505b9392505050565b600061175583611ae960201b60201c565b6117625760019050611ae3565b600083905060008173ffffffffffffffffffffffffffffffffffffffff16633850c7bd6040518163ffffffff1660e01b815260040160e060405180830381865afa1580156117b4573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906117d89190612458565b50505050505090506401000276a473ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff16111580611860575073fffd8963efd1fc6a506488495d951d5263988d2573ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff1610155b1561187057600192505050611ae3565b60008273ffffffffffffffffffffffffffffffffffffffff16630dfe16816040518163ffffffff1660e01b8152600401602060405180830381865afa1580156118bd573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906118e1919061206f565b905060008373ffffffffffffffffffffffffffffffffffffffff1663d21220a76040518163ffffffff1660e01b8152600401602060405180830381865afa158015611930573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190611954919061206f565b905060008273ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa1580156119a3573d6000803e3d6000fd5b505050506040513d601f19601f820116820180604052508101906119c7919061219d565b905060008273ffffffffffffffffffffffffffffffffffffffff1663313ce5676040518163ffffffff1660e01b8152600401602060405180830381865afa158015611a16573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190611a3a919061219d565b90506000611a4e8684611afc60201b60201c565b90508873ffffffffffffffffffffffffffffffffffffffff168573ffffffffffffffffffffffffffffffffffffffff1603611ab557816012611a9091906124fa565b600a611a9c91906122fd565b81611aa79190611cf0565b975050505050505050611ae3565b80826012611ac3919061252e565b600a611acf91906122fd565b611ad99190611cbf565b9750505050505050505b92915050565b600080823b905060008111915050919050565b6000808373ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff16611b379190611cf0565b9050600083600a611b489190612562565b9050611b7482827801000000000000000000000000000000000000000000000000611b7e60201b60201c565b9250505092915050565b6000806000801985870985870292508281108382030391505060008103611bb85760008411611bac57600080fd5b83820492505050611c50565b808411611bc457600080fd5b600084868809905082811182039150808303925060008586600003169050808604955080840493506001818260000304019050808302841793506000600287600302189050808702600203810290508087026002038102905080870260020381029050808702600203810290508087026002038102905080870260020381029050808502955050505050505b9392505050565b6000819050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601260045260246000fd5b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b6000611cca82611c57565b9150611cd583611c57565b925082611ce557611ce4611c61565b5b828204905092915050565b6000611cfb82611c57565b9150611d0683611c57565b9250828202611d1481611c57565b91508282048414831517611d2b57611d2a611c90565b5b5092915050565b611d3b81611c57565b82525050565b600061010082019050611d57600083018b611d32565b611d64602083018a611d32565b611d716040830189611d32565b611d7e6060830188611d32565b611d8b6080830187611d32565b611d9860a0830186611d32565b611da560c0830185611d32565b611db260e0830184611d32565b9998505050505050505050565b600081519050919050565b600081905092915050565b60005b83811015611df3578082015181840152602081019050611dd8565b60008484015250505050565b6000611e0a82611dbf565b611e148185611dca565b9350611e24818560208601611dd5565b80840191505092915050565b6000611e3c8285611dff565b9150611e488284611dff565b91508190509392505050565b600060c082019050611e696000830189611d32565b611e766020830188611d32565b611e836040830187611d32565b611e906060830186611d32565b611e9d6080830185611d32565b611eaa60a0830184611d32565b979650505050505050565b600060e082019050611eca600083018a611d32565b611ed76020830189611d32565b611ee46040830188611d32565b611ef16060830187611d32565b611efe6080830186611d32565b611f0b60a0830185611d32565b611f1860c0830184611d32565b98975050505050505050565b6000604082019050611f396000830185611d32565b611f466020830184611d32565b9392505050565b6000608082019050611f626000830187611d32565b611f6f6020830186611d32565b611f7c6040830185611d32565b611f896060830184611d32565b95945050505050565b6000611f9e8284611dff565b915081905092915050565b600080fd5b6000819050919050565b611fc181611fae565b8114611fcc57600080fd5b50565b600081519050611fde81611fb8565b92915050565b600060208284031215611ffa57611ff9611fa9565b5b600061200884828501611fcf565b91505092915050565b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b600061203c82612011565b9050919050565b61204c81612031565b811461205757600080fd5b50565b60008151905061206981612043565b92915050565b60006020828403121561208557612084611fa9565b5b60006120938482850161205a565b91505092915050565b60006dffffffffffffffffffffffffffff82169050919050565b6120bf8161209c565b81146120ca57600080fd5b50565b6000815190506120dc816120b6565b92915050565b600063ffffffff82169050919050565b6120fb816120e2565b811461210657600080fd5b50565b600081519050612118816120f2565b92915050565b60008060006060848603121561213757612136611fa9565b5b6000612145868287016120cd565b9350506020612156868287016120cd565b925050604061216786828701612109565b9150509250925092565b61217a81611c57565b811461218557600080fd5b50565b60008151905061219781612171565b92915050565b6000602082840312156121b3576121b2611fa9565b5b60006121c184828501612188565b91505092915050565b60008160011c9050919050565b6000808291508390505b6001851115612221578086048111156121fd576121fc611c90565b5b600185161561220c5780820291505b808102905061221a856121ca565b94506121e1565b94509492505050565b60008261223a57600190506122f6565b8161224857600090506122f6565b816001811461225e576002811461226857612297565b60019150506122f6565b60ff84111561227a57612279611c90565b5b8360020a91508482111561229157612290611c90565b5b506122f6565b5060208310610133831016604e8410600b84101617156122cc5782820a9050838111156122c7576122c6611c90565b5b6122f6565b6122d984848460016121d7565b925090508184048111156122f0576122ef611c90565b5b81810290505b9392505050565b600061230882611c57565b915061231383611c57565b92506123407fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff848461222a565b905092915050565b61235181612011565b811461235c57600080fd5b50565b60008151905061236e81612348565b92915050565b60008160020b9050919050565b61238a81612374565b811461239557600080fd5b50565b6000815190506123a781612381565b92915050565b600061ffff82169050919050565b6123c4816123ad565b81146123cf57600080fd5b50565b6000815190506123e1816123bb565b92915050565b600060ff82169050919050565b6123fd816123e7565b811461240857600080fd5b50565b60008151905061241a816123f4565b92915050565b60008115159050919050565b61243581612420565b811461244057600080fd5b50565b6000815190506124528161242c565b92915050565b600080600080600080600060e0888a03121561247757612476611fa9565b5b60006124858a828b0161235f565b97505060206124968a828b01612398565b96505060406124a78a828b016123d2565b95505060606124b88a828b016123d2565b94505060806124c98a828b016123d2565b93505060a06124da8a828b0161240b565b92505060c06124eb8a828b01612443565b91505092959891949750929550565b600061250582611c57565b915061251083611c57565b925082820390508181111561252857612527611c90565b5b92915050565b600061253982611c57565b915061254483611c57565b925082820190508082111561255c5761255b611c90565b5b92915050565b600061256d82611c57565b9150612578836123e7565b92506125a57fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff848461222a565b90509291505056fe";

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
            ("eth_alcx", ColumnType::Float64),
            ("eth_bnt", ColumnType::Float64),
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
            "eth_alcx",
            "eth_bnt",
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
            "eth_alcx".to_string(),
            "eth_bnt".to_string(),
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
                    let eth_alcx = format_ether(price_res[60]).parse::<f64>().unwrap();
                    let eth_bnt = format_ether(price_res[61]).parse::<f64>().unwrap();
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
                        eth_alcx,
                        eth_bnt
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
    eth_alcx: f64,
    eth_bnt: f64
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
    eth_alcx: Vec<f64>,
    eth_bnt: Vec<f64>
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
        if schema.has_column("eth_alcx") {
            self.eth_alcx.push(prices.eth_alcx);
        }
        if schema.has_column("eth_bnt") {
            self.eth_bnt.push(prices.eth_bnt);
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
        with_series!(cols, "eth_alcx", self.eth_alcx, schema);
        with_series!(cols, "eth_bnt", self.eth_bnt, schema);

        DataFrame::new(cols).map_err(CollectError::PolarsError).sort_by_schema(schema)
    }
}
