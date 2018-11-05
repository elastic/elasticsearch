# Support Elasticsearch landing data encryption

[TOC]

## Support for landing data encryption plugin

In the ElasticSearch server, the data in the storage is not encrypted when it is written to persistent storage. In the online generation environment, we have very high data security requirements. To this end, we have developed a storage component for data encryption that can support data encryption for sensitive parts of the landing data. The main starting point for us to do this is to consider that the Lucene open source community has discussed the issue of data security. It is through file system encryption, but this approach has a great impact on performance. To this end, we have considered performance and data security. The focus is on encrypting very sensitive landing data, such as tim, fdt, dvd, which contain sensitive information, and not encrypting other data files.


##Design 

In order to protect the landing data, it is physically dragged by the library or backed up by the backup. We consider the ability to design an encrypted landing data store and separate the user's master key from the login data. Once the node is started, the master key can only be obtained after a relatively strict authentication authorization (usually related to the company's internal key security mechanism). To this end, we designed and implemented the Store-encrypt plugin to support login data encryption and key docking management.

Features include

* Support AES-ECB mode encryption (128bit)
* Supports two key delivery methods, the data key is stored in the index settings, the encrypted data key is stored in the index settings and the docking master key is supported.
* Retain Elasticsearch's optional directory service capabilities
* Plug-in mode for flexible upgrade of ES and Lucene


## Getting Started

The ability to embed data encryption can be experienced in the following steps.

* Compile (optional). The corresponding version plugin can be downloaded directly.
* Installation
* Configuration

For details, please refer to the question.

## Compile

First, you need to download the plugin from the repository and compile the Elasticsearch project. For instructions on compiling, please refer to the Elasticsearch compilation process. After the configuration is complete, go to the root directory of elasticsearch and execute the following command to complete the compilation:

Gradlew assemble


If you do not want to compile, you can download the corresponding version and install it directly. Refer to the release version below for details.
Optional version:

* [Plugin For Elasticsearch 6.3.2] (http://)

## Installation

Please refer to the ElasticSearch plugin installation instructions. Details can be found below
Https://www.elastic.co/guide/en/elasticsearch/plugins/6.4/plugin-management-custom-url.html

## Configuration

1.Set Elasticsearch's lucene does not open file merge mode (optional)

	InternalEngine.java:1963 
	iwc.setUseCompoundFile(false); // always use compound on flush - reduces # of file-handles on refresh

2.Configure the storage type store type: index.store.type

By default, Elasticsearch supports optional file storage. Including fs, niofs, mmapfs, simplefs, please refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html for details. If you need encryption, just add the "encrypt\_" prefix to the file storage. If you want to use the default, use "encrypt_default". Therefore, the optional encryption "index.store.type" value includes:

* encrypt_default
* encrypt_fs
* encrypt_niofs
* ncrypt_mmapfs
* encrypt_simplefs

3.Configure the encryption key: index.directkey or index.encryptionkey

The key is 16 bytes (128 bit) long.

There are two ways to configure the key. If index.directkey has an assignment, it will be used first as the key for data encryption. Otherwise, index.encryptionkey will be used as the key for landing data encryption, but index.encryptionkey is not a plaintext key and needs to be encrypted by master key.

4.Configure the configuration file to get the master key encrypt.yml

	Encrypt:
	Dsmhost: https://mydsm.oa?user=xxx
	rootCA: config_path/dsm.crt

Configuration instructions, dsmhost is the requested dsm system host. The rootCA is a configurable root CA certificate path. After the system starts, it will request the path of dsmhost, and expect to return the data in the following format, so the user needs to implement the DSM system interface.

	{
		"dek" : "CpbCOtC9i0IuG5pBi+zl6R5iDZOSPSyxTs87zBiyLig=",
		"ret_code" : 0,
		"ret_msg" : ""
	}

The dek is a 16-byte 128-bit key encrypted by base64.


Example 1, by directly configuring the key, the value of the data key is (0123456789abcdef), and the base64 is encoded as (MDEyMzQ1Njc4OWFiY2RlZg==).

	PUT twitter
	{
		"settings" : {
			"index" : {
				"directkey":"MDEyMzQ1Njc4OWFiY2RlZg==",
				"store":{
					"type": "encrypt_fs"
					}
			}
		}
	}

Example 2, by directly configuring the key, the value of the data key is (0123456789abcdef), and the base64 value of the data key encrypted by the master key is (KpltMAJDFYrjvy/ohXmr4Q==), and the master key is requested by the DSM system. The value of the key is (aaaaaaaaaaffffff).

	PUT twitter
	{
		"settings" : {
		"index" : {
			"encryptionkey":"KpltMAJDFYrjvy/ohXmr4Q==",
				"store":{
					"type": "encrypt_fs"
					}
			}
		}
	}
	
	The DSM system should implement an interface that returns the following. (YWFhYWFhYWFhYWZmZmZmZg==) is the base64 value of (aaaaaaaaaaffffff).
	{
		"dek" : "YWFhYWFhYWFhYWZmZmZmZg==",
		"ret_code" : 0,
		"ret_msg" : ""
	}

For more information about


        
## Safety instructions

Index.store.type
Https://www.elastic.co/guide/en/elasticsearch/reference/6.4/breaking-changes-6.0.html