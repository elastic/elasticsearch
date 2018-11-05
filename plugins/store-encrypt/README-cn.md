# 支持Elasticsearch落地数据加密

[TOC]

## 支持落地数据加密的插件

在ElasticSearch服务器中，存储中的数据在写入持久存储时不会被加密。在线上生成环境中，我们有非常高的数据安全要求，为此，我们开发了一个用于数据加密的存储组件，可以支持对落地数据的敏感部分进行数据加密。我们这样做的主要出发点，是考虑到lucene开源社区曾经讨论过落地数据安全的一个建议是通过文件系统加密，但这种做法对性能影响较大，为此，我们综合考虑了性能和落地数据安全的侧重点，对非常敏感的落地数据进行加密，比如tim、fdt、dvd这几种包含敏感信息的文件，同时对于其它数据文件不加密。


## 设计

为了保护落地数据被物理拖动库或备份窃取。 我们考虑设计加密的着陆数据存储并将用户的主密钥与登陆数据分开的能力。 启动节点后，只有在相对严格的身份验证授权（通常与公司的内部密钥安全机制相关）后才能获取主密钥。 为此，我们设计并实现了Store-encrypt插件，以支持登陆数据加密和密钥对接管理。

功能包括

* 支持AES-ECB模式加密（128bit）
* 支持2种密钥传递方法，数据密钥存储在index settings、被加密的数据密钥存储在 index settings并支持对接主密钥
* 保留Elasticsearch可选目录服务能力
* 插件模式，可灵活对ES、lucene升级


## 开始 Getting Started

通过以下几个步骤就可以体验落地数据加密的能力。

* 编译（可选）。可直接下载对应的版本plugin
* 安装
* 配置

详情参考后问

## 编译 

首先，需要从仓库下载该插件，并编译Elasticsearch项目。编译的说明，请参考Elasticsearch的编译流程。配置完成后，进入elasticsearch的根目录，执行以下命令，即可完成编译：

	gradlew assemble


如果不想编译，可以下载对应的版本直接安装即可。详情参考以下的发布版本。
可选版本：

* [Plugin For Elasticsearch 6.3.2](http://)

## 安装 Installation

请参考  ElasticSearch 的插件安装指令。详情可以从下面获得参考
https://www.elastic.co/guide/en/elasticsearch/plugins/6.4/plugin-management-custom-url.html

## 配置 Configuration 

1.设置Elasticsearch的lucene不开启文件合并模式（可选）
		
		setUseCompoundFile

2.配置存储类型store type： index.store.type 

默认情况下，Elasticsearch支持可选的文件存储。包括 fs、niofs、mmapfs、simplefs，详情可参考 https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html 。如果需要加密，只需要在文件存储下加上 "encrypt\_" 前缀，如果想用默认，用 "encrypt_default" 即可。因此，可选的加密  "index.store.type" 值包括：

* encrypt_default 
* encrypt_fs
* encrypt_niofs
* ncrypt_mmapfs
* encrypt_simplefs

3.配置加密密钥：index.directkey 或者 index.encryptionkey

密钥为16字节（128 bit）的长度。

有2种配置密钥的方式。如果 index.directkey 有赋值，将优先使用该值作为数据加密的密钥。否则， 将用 index.encryptionkey 作为落地数据加密的密钥，但是  index.encryptionkey 并不是明文密钥，需要通过主密钥加密才能获得。

4.配置获取主密钥的配置文件 encrypt.yml

	encrypt:
	    dsmhost: https://mydsm.oa?user=xxx
	    rootCA:  config_path/dsm.crt 

配置说明，dsmhost是请求的dsm系统host。rootCA是可配置的根CA证书路径。系统启动后，会请求dsmhost的路径，并期望返回以下格式的数据，因此需要用户自行实现DSM系统接口

	{
	   "dek" : "CpbCOtC9i0IuG5pBi+zl6R5iDZOSPSyxTs87zBiyLig=",
	   "ret_code" : 0,
	   "ret_msg" : ""
	}
	
其中 dek是base64加密后的16字节128位密钥。


例子1，通过直接配置密钥，数据密钥的值为（0123456789abcdef），base64编码后为（MDEyMzQ1Njc4OWFiY2RlZg==）。

	PUT twitter
	{
	    "settings" : {
	        "index" : {
		        "directkey":"MDEyMzQ1Njc4OWFiY2RlZg==",
		        "store":{
					"type":"encrypt_fs"
				}
	        }
	    }
	}

例子2，通过直接配置密钥，数据密钥的值为（0123456789abcdef），数据密钥被主密钥加密后的base64值为（KpltMAJDFYrjvy/ohXmr4Q==），主密钥通过DSM系统请求获取，主密钥的值为（aaaaaaaaaaffffff）。

	PUT twitter
	{
	    "settings" : {
	        "index" : {
		        "encryptionkey":"KpltMAJDFYrjvy/ohXmr4Q==",
		        "store":{
					"type":"encrypt_fs"
				}
	        }
	    }
	}

	DSM 系统应该实现返回以下内容的接口。（YWFhYWFhYWFhYWZmZmZmZg==）是（aaaaaaaaaaffffff）的base64值。
	{
	   "dek" : "YWFhYWFhYWFhYWZmZmZmZg==",
	   "ret_code" : 0,
	   "ret_msg" : ""
	}

for more information about

	
        
## 安全说明



index.store.type
https://www.elastic.co/guide/en/elasticsearch/reference/6.4/breaking-changes-6.0.html 
