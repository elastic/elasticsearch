---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/saml-metadata.html
---

# elasticsearch-saml-metadata [saml-metadata]

The `elasticsearch-saml-metadata` command can be used to generate a SAML 2.0 Service Provider Metadata file.


## Synopsis [_synopsis_8]

```shell
bin/elasticsearch-saml-metadata
[--realm <name>]
[--out <file_path>] [--batch]
[--attribute <name>] [--service-name <name>]
[--locale <name>] [--contacts]
([--organisation-name <name>] [--organisation-display-name <name>] [--organisation-url <url>])
([--signing-bundle <file_path>] | [--signing-cert <file_path>][--signing-key <file_path>])
[--signing-key-password <password>]
[-E <KeyValuePair>]
[-h, --help] ([-s, --silent] | [-v, --verbose])
```


## Description [_description_15]

The SAML 2.0 specification provides a mechanism for Service Providers to describe their capabilities and configuration using a *metadata file*.

The `elasticsearch-saml-metadata` command generates such a file, based on the configuration of a SAML realm in {{es}}.

Some SAML Identity Providers will allow you to automatically import a metadata file when you configure the Elastic Stack as a Service Provider.

You can optionally select to digitally sign the metadata file in order to ensure its integrity and authenticity before sharing it with the Identity Provider. The key used for signing the metadata file need not necessarily be the same as the keys already used in the saml realm configuration for SAML message signing.

If your {{es}} keystore is password protected, you are prompted to enter the password when you run the `elasticsearch-saml-metadata` command.


## Parameters [saml-metadata-parameters]

`--attribute <name>`
:   Specifies a SAML attribute that should be included as a `<RequestedAttribute>` element in the metadata. Any attribute configured in the {{es}} realm is automatically included and does not need to be specified as a commandline option.

`--batch`
:   Do not prompt for user input.

`--contacts`
:   Specifies that the metadata should include one or more `<ContactPerson>` elements. The user will be prompted to enter the details for each person.

`-E <KeyValuePair>`
:   Configures an {{es}} setting.

`-h, --help`
:   Returns all of the command parameters.

`--locale <name>`
:   Specifies the locale to use for metadata elements such as `<ServiceName>`. Defaults to the JVM’s default system locale.

`--organisation-display-name <name`
:   Specified the value of the `<OrganizationDisplayName>` element. Only valid if `--organisation-name` is also specified.

`--organisation-name <name>`
:   Specifies that an `<Organization>` element should be included in the metadata and provides the value for the `<OrganizationName>`. If this is specified, then `--organisation-url` must also be specified.

`--organisation-url <url>`
:   Specifies the value of the `<OrganizationURL>` element. This is required if `--organisation-name` is specified.

`--out <file_path>`
:   Specifies a path for the output files. Defaults to `saml-elasticsearch-metadata.xml`

`--service-name <name>`
:   Specifies the value for the `<ServiceName>` element in the metadata. Defaults to `elasticsearch`.

`--signing-bundle <file_path>`
:   Specifies the path to an existing key pair (in PKCS#12 format). The private key of that key pair will be used to sign the metadata file.

`--signing-cert <file_path>`
:   Specifies the path to an existing certificate (in PEM format) to be used for signing of the metadata file. You must also specify the `--signing-key` parameter. This parameter cannot be used with the `--signing-bundle` parameter.

`--signing-key <file_path>`
:   Specifies the path to an existing key (in PEM format) to be used for signing of the metadata file. You must also specify the `--signing-cert` parameter. This parameter cannot be used with the `--signing-bundle` parameter.

`--signing-key-password <password>`
:   Specifies the password for the signing key. It can be used with either the `--signing-key` or the `--signing-bundle` parameters.

`--realm <name>`
:   Specifies the name of the realm for which the metadata should be generated. This parameter is required if there is more than 1 `saml` realm in your {{es}} configuration.

`-s, --silent`
:   Shows minimal output.

`-v, --verbose`
:   Shows verbose output.


## Examples [_examples_20]

The following command generates a default metadata file for the `saml1` realm:

```sh
bin/elasticsearch-saml-metadata --realm saml1
```

The file will be written to `saml-elasticsearch-metadata.xml`. You may be prompted to provide the "friendlyName" value for any attributes that are used by the realm.

The following command generates a metadata file for the `saml2` realm, with a `<ServiceName>` of `kibana-finance`, a locale of `en-GB` and includes `<ContactPerson>` elements and an `<Organization>` element:

```sh
bin/elasticsearch-saml-metadata --realm saml2 \
    --service-name kibana-finance \
    --locale en-GB \
    --contacts \
    --organisation-name "Mega Corp. Finance Team" \
    --organisation-url "http://mega.example.com/finance/"
```

