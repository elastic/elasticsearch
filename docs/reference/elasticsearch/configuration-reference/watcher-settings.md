---
navigation_title: "{{watcher}} settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/notification-settings.html
applies_to:
  deployment:
    ess:
    self:
---

# {{watcher}} settings in Elasticsearch [notification-settings]


$$$notification-settings-description$$$
You configure {{watcher}} settings to set up {{watcher}} and send notifications via [email](#email-notification-settings), [Slack](#slack-notification-settings), and [PagerDuty](#pagerduty-notification-settings).

All of these settings can be added to the `elasticsearch.yml` configuration file, with the exception of the secure settings, which you add to the {{es}} keystore. For more information about creating and updating the {{es}} keystore, see [Secure settings](docs-content://deploy-manage/security/secure-settings.md). Dynamic settings can also be updated across a cluster with the [cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings).

## General Watcher Settings [general-notification-settings]

`xpack.watcher.enabled`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set to `false` to disable {{watcher}} on the node.

$$$xpack-watcher-encrypt-sensitive-data$$$

`xpack.watcher.encrypt_sensitive_data` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set to `true` to encrypt sensitive data. If this setting is enabled, you must also specify the `xpack.watcher.encryption_key` setting. For more information, see [*Encrypting sensitive data in {{watcher}}*](docs-content://explore-analyze/alerts-cases/watcher/encrypting-data.md).

`xpack.watcher.encryption_key`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Specifies the path to a file that contains a key for encrypting sensitive data. If `xpack.watcher.encrypt_sensitive_data` is set to `true`, this setting is required. For more information, see [*Encrypting sensitive data in {{watcher}}*](docs-content://explore-analyze/alerts-cases/watcher/encrypting-data.md).

`xpack.watcher.max.history.record.size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The maximum size watcher history record that can be written into the watcher history index. Any larger history record will have some of its larger fields removed. Defaults to 10mb.

`xpack.watcher.trigger.schedule.engine` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Defines when the watch should start, based on date and time [Learn more](docs-content://explore-analyze/alerts-cases/watcher/schedule-types.md).

`xpack.watcher.history.cleaner_service.enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Controls [whether old watcher indices are automatically deleted](/reference/elasticsearch/configuration-reference/watcher-settings.md#general-notification-settings).

`xpack.http.proxy.host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the address of the proxy server to use to connect to HTTP services.

`xpack.http.proxy.port`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the port number to use to connect to the proxy server.

`xpack.http.proxy.scheme`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Protocol used to communicate with the proxy server. Valid values are `http` and `https`. Defaults to the protocol used in the request.

`xpack.http.default_connection_timeout`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The maximum period to wait until abortion of the request, when a connection is being initiated.

`xpack.http.default_read_timeout`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The maximum period of inactivity between two data packets, before the request is aborted.

`xpack.http.tcp.keep_alive`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Whether to enable TCP keepalives on HTTP connections. Defaults to `true`.

`xpack.http.connection_pool_ttl`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The time-to-live of connections in the connection pool. If a connection is not re-used within this timeout, it is closed. By default, the time-to-live is infinite meaning that connections never expire.

`xpack.http.max_response_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the maximum size an HTTP response is allowed to have, defaults to `10mb`, the maximum configurable value is `50mb`.

`xpack.http.whitelist`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A list of URLs, that the internal HTTP client is allowed to connect to. This client is used in the HTTP input, the webhook, the slack, pagerduty, and jira actions. This setting can be updated dynamically. It defaults to `*` allowing everything. Note: If you configure this setting and you are using one of the slack/pagerduty actions, you have to ensure that the corresponding endpoints are explicitly allowed as well.


## {{watcher}} HTTP TLS/SSL settings [ssl-notification-settings]

You can configure the following TLS/SSL settings.

`xpack.http.ssl.supported_protocols`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Supported protocols with versions. Valid protocols: `SSLv2Hello`, `SSLv3`, `TLSv1`, `TLSv1.1`, `TLSv1.2`, `TLSv1.3`. If the JVM’s SSL provider supports TLSv1.3, the default is `TLSv1.3,TLSv1.2,TLSv1.1`. Otherwise, the default is `TLSv1.2,TLSv1.1`.

    {{es}} relies on your JDK’s implementation of SSL and TLS. View [Supported SSL/TLS versions by JDK version](docs-content://deploy-manage/security/supported-ssltls-versions-by-jdk-version.md) for more information.

    ::::{note}
    If `xpack.security.fips_mode.enabled` is `true`, you cannot use `SSLv2Hello` or `SSLv3`. See [FIPS 140-2](docs-content://deploy-manage/security/fips-140-2.md).
    ::::


`xpack.http.ssl.verification_mode`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Defines how to verify the certificates presented by another party in the TLS connection:

    Defaults to `full`.

    **Valid values**
    * `full`:   Validates that the provided certificate: has an issue date that’s within the `not_before` and `not_after` dates; chains to a trusted Certificate Authority (CA); has a `hostname` or IP address that matches the names within the certificate.
    * `certificate`:   Validates the provided certificate and verifies that it’s signed by a trusted authority (CA), but doesn’t check the certificate `hostname`.
    * `none`:   Performs no certificate validation.

      ::::{important}
      Setting certificate validation to `none` disables many security benefits of SSL/TLS, which is very dangerous. Only set this value if instructed by Elastic Support as a temporary diagnostic mechanism when attempting to resolve TLS errors.
      ::::

`xpack.http.ssl.cipher_suites` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Supported cipher suites vary depending on which version of Java you use. For example, for version 12 the default value is `TLS_AES_256_GCM_SHA384`, `TLS_AES_128_GCM_SHA256`, `TLS_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384`, `TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA`, `TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA`, `TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA`, `TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA`, `TLS_RSA_WITH_AES_256_GCM_SHA384`, `TLS_RSA_WITH_AES_128_GCM_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA256`, `TLS_RSA_WITH_AES_128_CBC_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA`, `TLS_RSA_WITH_AES_128_CBC_SHA`.

    For more information, see Oracle’s [Java Cryptography Architecture documentation](https://docs.oracle.com/en/java/javase/11/security/oracle-providers.md#GUID-7093246A-31A3-4304-AC5F-5FB6400405E2).


### {{watcher}} HTTP TLS/SSL key and trusted certificate settings [watcher-tls-ssl-key-trusted-certificate-settings]

The following settings are used to specify a private key, certificate, and the trusted certificates that should be used when communicating over an SSL/TLS connection. A private key and certificate are optional and would be used if the server requires client authentication for PKI authentication.


### PEM encoded files [_pem_encoded_files_6]

When using PEM encoded files, use the following settings:

`xpack.http.ssl.key`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Path to a PEM encoded file containing the private key.

    If HTTP client authentication is required, it uses this file. You cannot use this setting and `ssl.keystore.path` at the same time.


`xpack.http.ssl.secure_key_passphrase`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The passphrase that is used to decrypt the private key. Since the key might not be encrypted, this value is optional.

`xpack.http.ssl.certificate`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the path for the PEM encoded certificate (or certificate chain) that is associated with the key.

    This setting can be used only if `ssl.key` is set.


`xpack.http.ssl.certificate_authorities`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) List of paths to PEM encoded certificate files that should be trusted.

    This setting and `ssl.truststore.path` cannot be used at the same time.



### Java keystore files [_java_keystore_files_6]

When using Java keystore files (JKS), which contain the private key, certificate and certificates that should be trusted, use the following settings:

`xpack.http.ssl.keystore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore file that contains a private key and certificate.

    It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `ssl.key` at the same time.


`xpack.http.ssl.keystore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the keystore.

`xpack.http.ssl.keystore.secure_key_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the key in the keystore. The default is the keystore password.

`xpack.http.ssl.truststore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore that contains the certificates to trust. It must be either a Java keystore (jks) or a PKCS#12 file.

    You cannot use this setting and `ssl.certificate_authorities` at the same time.


`xpack.http.ssl.truststore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Password for the truststore.


### PKCS#12 files [watcher-pkcs12-files]

{{es}} can be configured to use PKCS#12 container files (`.p12` or `.pfx` files) that contain the private key, certificate and certificates that should be trusted.

PKCS#12 files are configured in the same way as Java keystore files:

`xpack.http.ssl.keystore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore file that contains a private key and certificate.

    It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `ssl.key` at the same time.


`xpack.http.ssl.keystore.type`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The format of the keystore file. It must be either `jks` or `PKCS12`. If the keystore path ends in ".p12", ".pfx", or ".pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`xpack.http.ssl.keystore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the keystore.

`xpack.http.ssl.keystore.secure_key_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the key in the keystore. The default is the keystore password.

`xpack.http.ssl.truststore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore that contains the certificates to trust. It must be either a Java keystore (jks) or a PKCS#12 file.

    You cannot use this setting and `ssl.certificate_authorities` at the same time.


`xpack.http.ssl.truststore.type`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set this to `PKCS12` to indicate that the truststore is a PKCS#12 file.

`xpack.http.ssl.truststore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Password for the truststore.



## Email Notification Settings [email-notification-settings]

You can configure the following email notification settings in `elasticsearch.yml`. For more information about sending notifications via email, see [Configuring email actions](docs-content://explore-analyze/alerts-cases/watcher/actions-email.md#configuring-email-actions).

`xpack.notification.email.default_account`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Default email account to use.

    If you configure multiple email accounts, you must either configure this setting or specify the email account to use in the [`email`](docs-content://explore-analyze/alerts-cases/watcher/actions-email.md) action. See [Configuring email accounts](docs-content://explore-analyze/alerts-cases/watcher/actions-email.md#configuring-email).


`xpack.notification.email.recipient_allowlist`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies addresses to which emails are allowed to be sent. Emails with recipients (`To:`, `Cc:`, or `Bcc:`) outside of these patterns will be rejected and an error thrown. This setting defaults to `["*"]` which means all recipients are allowed. Simple globbing is supported, such as `list-*@company.com` in the list of allowed recipients.

::::{note}
This setting can’t be used at the same time as `xpack.notification.email.account.domain_allowlist` and an error will be thrown if both are set at the same time. This setting can be used to specify domains to allow by using a wildcard pattern such as `*@company.com`.
::::


`xpack.notification.email.account`
:   Specifies account information for sending notifications via email. You can specify the following email account attributes:

`xpack.notification.email.account.domain_allowlist`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies domains to which emails are allowed to be sent. Emails with recipients (`To:`, `Cc:`, or `Bcc:`) outside of these domains will be rejected and an error thrown. This setting defaults to `["*"]` which means all domains are allowed. Simple globbing is supported, such as `*.company.com` in the list of allowed domains.

::::{note}
This setting can’t be used at the same time as `xpack.notification.email.recipient_allowlist` and an error will be thrown if both are set at the same time.
::::


$$$email-account-attributes$$$

`profile`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The [email profile](docs-content://explore-analyze/alerts-cases/watcher/actions-email.md#configuring-email) to use to build the MIME messages that are sent from the account. Valid values: `standard`, `gmail` and `outlook`. Defaults to `standard`.

`email_defaults.*`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) An optional set of email attributes to use as defaults for the emails sent from the account. See [Email action attributes](docs-content://explore-analyze/alerts-cases/watcher/actions-email.md#email-action-attributes) for the supported attributes.

`smtp.auth`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Set to `true` to attempt to authenticate the user using the AUTH command. Defaults to `false`.

`smtp.host`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The SMTP server to connect to. Required.

`smtp.port`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The SMTP server port to connect to. Defaults to 25.

`smtp.user`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The user name for SMTP. Required.

`smtp.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) The password for the specified SMTP user.

`smtp.starttls.enable`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Set to `true` to enable the use of the `STARTTLS` command (if supported by the server) to switch the connection to a TLS-protected connection before issuing any login commands. Note that an appropriate trust store must be configured so that the client will trust the server’s certificate. Defaults to `false`.

`smtp.starttls.required`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) If `true`, then `STARTTLS` will be required. If that command fails, the connection will fail. Defaults to `false`.

`smtp.ssl.trust`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A list of SMTP server hosts that are assumed trusted and for which certificate verification is disabled. If set to "*", all hosts are trusted. If set to a whitespace separated list of hosts, those hosts are trusted. Otherwise, trust depends on the certificate the server presents.

`smtp.timeout`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The socket read timeout. Default is two minutes.

`smtp.connection_timeout`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The socket connection timeout. Default is two minutes.

`smtp.write_timeout`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The socket write timeout. Default is two minutes.

`smtp.local_address`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A configurable local address when sending emails. Not configured by default.

`smtp.local_port`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A configurable local port when sending emails. Not configured by default.

`smtp.send_partial`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Send an email, despite one of the receiver addresses being invalid.

`smtp.wait_on_quit`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) If set to false the QUIT command is sent and the connection closed. If set to true, the QUIT command is sent and a reply is waited for. True by default.

`xpack.notification.email.html.sanitization.allow` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies the HTML elements that are allowed in email notifications. For more information, see [Configuring HTML sanitization options](docs-content://explore-analyze/alerts-cases/watcher/actions-email.md#email-html-sanitization). You can specify individual HTML elements and the following HTML feature groups:

    $$$html-feature-groups$$$

    `_tables`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) All table related elements: `<table>`, `<th>`, `<tr>`, `<td>`, `<caption>`, `<col>`, `<colgroup>`, `<thead>`, `<tbody>`, and `<tfoot>`.

    `_blocks`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The following block elements: `<p>`, `<div>`, `<h1>`, `<h2>`, `<h3>`, `<h4>`, `<h5>`, `<h6>`, `<ul>`, `<ol>`, `<li>`, and `<blockquote>`.

    `_formatting`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The following inline formatting elements: `<b>`, `<i>`, `<s>`, `<u>`, `<o>`, `<sup>`, `<sub>`, `<ins>`, `<del>`, `<strong>`, `<strike>`, `<tt>`, `<code>`, `<big>`, `<small>`, `<hr>`, `<br>`, `<span>`, and `<em>`.

    `_links`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The `<a>` element with an `href` attribute that points to a URL using the following protocols: `http`, `https` and `mailto`.

    `_styles`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The `style` attribute on all elements. Note that CSS attributes are also sanitized to prevent XSS attacks.

    `img`
    `img:all`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) All images (external and embedded).

    `img:embedded`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Only embedded images. Embedded images can only use the `cid:` URL protocol in their `src` attribute.


`xpack.notification.email.html.sanitization.disallow` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the HTML elements that are NOT allowed in email notifications. You can specify individual HTML elements and [HTML feature groups](#html-feature-groups).

`xpack.notification.email.html.sanitization.enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set to `false` to completely disable HTML sanitation. Not recommended. Defaults to `true`.

`xpack.notification.reporting.warning.kbn-csv-contains-formulas.text`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Specifies a custom message, which is sent if the formula verification criteria for CSV files from {{kib}}'s [`xpack.reporting.csv.checkForFormulas`](kibana://reference/configuration-reference/reporting-settings.md#reporting-csv-settings) is `true`. Use `%s` in the message as a placeholder for the filename. Defaults to `Warning: The attachment [%s] contains characters which spreadsheet applications may interpret as formulas. Please ensure that the attachment is safe prior to opening.`


## {{watcher}} Email TLS/SSL settings [ssl-notification-smtp-settings]

You can configure the following TLS/SSL settings.

`xpack.notification.email.ssl.supported_protocols`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Supported protocols with versions. Valid protocols: `SSLv2Hello`, `SSLv3`, `TLSv1`, `TLSv1.1`, `TLSv1.2`, `TLSv1.3`. If the JVM’s SSL provider supports TLSv1.3, the default is `TLSv1.3,TLSv1.2,TLSv1.1`. Otherwise, the default is `TLSv1.2,TLSv1.1`.

    {{es}} relies on your JDK’s implementation of SSL and TLS. View [Supported SSL/TLS versions by JDK version](docs-content://deploy-manage/security/supported-ssltls-versions-by-jdk-version.md) for more information.

    ::::{note}
    If `xpack.security.fips_mode.enabled` is `true`, you cannot use `SSLv2Hello` or `SSLv3`. See [FIPS 140-2](docs-content://deploy-manage/security/fips-140-2.md).
    ::::


`xpack.notification.email.ssl.verification_mode`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Defines how to verify the certificates presented by another party in the TLS connection:

    Defaults to `full`.

    **Valid values**:
    * `full`:   Validates that the provided certificate: has an issue date that’s within the `not_before` and `not_after` dates; chains to a trusted Certificate Authority (CA); has a `hostname` or IP address that matches the names within the certificate.
    * `certificate`:   Validates the provided certificate and verifies that it’s signed by a trusted authority (CA), but doesn’t check the certificate `hostname`.
    * `none`:   Performs no certificate validation.

      ::::{important}
      Setting certificate validation to `none` disables many security benefits of SSL/TLS, which is very dangerous. Only set this value if instructed by Elastic Support as a temporary diagnostic mechanism when attempting to resolve TLS errors.
      ::::

`xpack.notification.email.ssl.cipher_suites`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Supported cipher suites vary depending on which version of Java you use. For example, for version 12 the default value is `TLS_AES_256_GCM_SHA384`, `TLS_AES_128_GCM_SHA256`, `TLS_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384`, `TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA`, `TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA`, `TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA`, `TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA`, `TLS_RSA_WITH_AES_256_GCM_SHA384`, `TLS_RSA_WITH_AES_128_GCM_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA256`, `TLS_RSA_WITH_AES_128_CBC_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA`, `TLS_RSA_WITH_AES_128_CBC_SHA`.

    For more information, see Oracle’s [Java Cryptography Architecture documentation](https://docs.oracle.com/en/java/javase/11/security/oracle-providers.md#GUID-7093246A-31A3-4304-AC5F-5FB6400405E2).


### {{watcher}} Email TLS/SSL key and trusted certificate settings [watcher-email-tls-ssl-key-trusted-certificate-settings]

The following settings are used to specify a private key, certificate, and the trusted certificates that should be used when communicating over an SSL/TLS connection. A private key and certificate are optional and would be used if the server requires client authentication for PKI authentication.


### PEM encoded files [_pem_encoded_files_7]

When using PEM encoded files, use the following settings:

`xpack.notification.email.ssl.key`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Path to a PEM encoded file containing the private key.

    If HTTP client authentication is required, it uses this file. You cannot use this setting and `ssl.keystore.path` at the same time.


`xpack.notification.email.ssl.secure_key_passphrase`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The passphrase that is used to decrypt the private key. Since the key might not be encrypted, this value is optional.

`xpack.notification.email.ssl.certificate`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the path for the PEM encoded certificate (or certificate chain) that is associated with the key.

    This setting can be used only if `ssl.key` is set.


`xpack.notification.email.ssl.certificate_authorities`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) List of paths to PEM encoded certificate files that should be trusted.

    This setting and `ssl.truststore.path` cannot be used at the same time.



### Java keystore files [_java_keystore_files_7]

When using Java keystore files (JKS), which contain the private key, certificate and certificates that should be trusted, use the following settings:

`xpack.notification.email.ssl.keystore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore file that contains a private key and certificate.

    It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `ssl.key` at the same time.


`xpack.notification.email.ssl.keystore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the keystore.

`xpack.notification.email.ssl.keystore.secure_key_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the key in the keystore. The default is the keystore password.

`xpack.notification.email.ssl.truststore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore that contains the certificates to trust. It must be either a Java keystore (jks) or a PKCS#12 file.

    You cannot use this setting and `ssl.certificate_authorities` at the same time.


`xpack.notification.email.ssl.truststore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Password for the truststore.


### PKCS#12 files [watcher-email-pkcs12-files]

{{es}} can be configured to use PKCS#12 container files (`.p12` or `.pfx` files) that contain the private key, certificate and certificates that should be trusted.

PKCS#12 files are configured in the same way as Java keystore files:

`xpack.notification.email.ssl.keystore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore file that contains a private key and certificate.

    It must be either a Java keystore (jks) or a PKCS#12 file. You cannot use this setting and `ssl.key` at the same time.


`xpack.notification.email.ssl.keystore.type`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The format of the keystore file. It must be either `jks` or `PKCS12`. If the keystore path ends in ".p12", ".pfx", or ".pkcs12", this setting defaults to `PKCS12`. Otherwise, it defaults to `jks`.

`xpack.notification.email.ssl.keystore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the keystore.

`xpack.notification.email.ssl.keystore.secure_key_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) The password for the key in the keystore. The default is the keystore password.

`xpack.notification.email.ssl.truststore.path`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The path for the keystore that contains the certificates to trust. It must be either a Java keystore (jks) or a PKCS#12 file.

    You cannot use this setting and `ssl.certificate_authorities` at the same time.


`xpack.notification.email.ssl.truststore.type`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set this to `PKCS12` to indicate that the truststore is a PKCS#12 file.

`xpack.notification.email.ssl.truststore.secure_password`
:   ([Secure](docs-content://deploy-manage/security/secure-settings.md)) Password for the truststore.



## Slack Notification Settings [slack-notification-settings]

You can configure the following Slack notification settings in `elasticsearch.yml`. For more information about sending notifications via Slack, see [Configuring Slack actions](docs-content://explore-analyze/alerts-cases/watcher/actions-slack.md#configuring-slack-actions).

`xpack.notification.slack` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Configures [Slack notification settings](docs-content://explore-analyze/alerts-cases/watcher/actions-slack.md). Note that you need to add `secure_url` as a [secret value to the keystore](docs-content://deploy-manage/security/secure-settings.md).

`xpack.notification.slack.default_account` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Default Slack account to use.

    If you configure multiple Slack accounts, you must either configure this setting or specify the Slack account to use in the [`slack`](docs-content://explore-analyze/alerts-cases/watcher/actions-slack.md) action. See [Configuring Slack Accounts](docs-content://explore-analyze/alerts-cases/watcher/actions-slack.md#configuring-slack).


$$$slack-account-attributes$$$

`xpack.notification.slack.account` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies account information for sending notifications via Slack. You can specify the following Slack account attributes:

    `secure_url`
    :   ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) The Incoming Webhook URL to use to post messages to Slack. Required.

    `message_defaults`
    :   Default values for [Slack message attributes](docs-content://explore-analyze/alerts-cases/watcher/actions-slack.md#slack-action-attributes).

        `from`
        :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The sender name to display in the Slack message. Defaults to the watch ID.

        `to`
        :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The default Slack channels or groups you want to send messages to.

        `icon`
        :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The icon to display in the Slack messages. Overrides the incoming webhook’s configured icon. Accepts a public URL to an image.

        `text`
        :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The default message content.

        `attachment`
        :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Default message attachments. Slack message attachments enable you to create more richly-formatted messages. Specified as an array as defined in the [ Slack attachments documentation](https://api.slack.com/docs/attachments).



## Jira Notification Settings [jira-notification-settings]

You can configure the following Jira notification settings in `elasticsearch.yml`. For more information about using notifications to create issues in Jira, see [Configuring Jira actions](docs-content://explore-analyze/alerts-cases/watcher/actions-jira.md#configuring-jira-actions).

`xpack.notification.jira.default_account`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Default Jira account to use.

    If you configure multiple Jira accounts, you must either configure this setting or specify the Jira account to use in the [`jira`](docs-content://explore-analyze/alerts-cases/watcher/actions-jira.md) action. See [Configuring Jira accounts](docs-content://explore-analyze/alerts-cases/watcher/actions-jira.md#configuring-jira).


$$$jira-account-attributes$$$

`xpack.notification.jira.account`
:   Specifies account information for using notifications to create issues in Jira. You can specify the following Jira account attributes:

    `allow_http`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) If `false`, Watcher rejects URL settings that use a HTTP protocol. Defaults to `false`.

    `secure_url`
    :   ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) The URL of the Jira Software server. Required.

    `secure_user`
    :   ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) The name of the user to connect to the Jira Software server. Required.

    `secure_password`
    :   ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) The password of the user to connect to the Jira Software server. Required.

    `issue_defaults`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Default fields values for the issue created in Jira. See [Jira action attributes](docs-content://explore-analyze/alerts-cases/watcher/actions-jira.md#jira-action-attributes) for more information. Optional.



## PagerDuty Notification Settings [pagerduty-notification-settings]

You can configure the following PagerDuty notification settings in `elasticsearch.yml`. For more information about sending notifications via PagerDuty, see [Configuring PagerDuty actions](docs-content://explore-analyze/alerts-cases/watcher/actions-pagerduty.md#configuring-pagerduty-actions).

`xpack.notification.pagerduty` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Configures [PagerDuty notification settings](docs-content://explore-analyze/alerts-cases/watcher/actions-pagerduty.md#configuring-pagerduty).

`xpack.notification.pagerduty.default_account` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Default PagerDuty account to use.

    If you configure multiple PagerDuty accounts, you must either configure this setting or specify the PagerDuty account to use in the [`pagerduty`](docs-content://explore-analyze/alerts-cases/watcher/actions-pagerduty.md) action. See [Configuring PagerDuty accounts](docs-content://explore-analyze/alerts-cases/watcher/actions-pagerduty.md#configuring-pagerduty).


$$$pagerduty-account-attributes$$$

`xpack.notification.pagerduty.account` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Specifies account information for sending notifications via PagerDuty. You can specify the following PagerDuty account attributes:

    `name`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) A name for the PagerDuty account associated with the API key you are using to access PagerDuty. Required.

    `secure_service_api_key`
    :   ([Secure](docs-content://deploy-manage/security/secure-settings.md), [reloadable](docs-content://deploy-manage/security/secure-settings.md#reloadable-secure-settings)) The [ PagerDuty API key](https://developer.pagerduty.com/documentation/rest/authentication) to use to access PagerDuty. Required.


`event_defaults`
:   Default values for [PagerDuty event attributes](docs-content://explore-analyze/alerts-cases/watcher/actions-pagerduty.md#pagerduty-event-trigger-incident-attributes). Optional.

    `description`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A string that contains the default description for PagerDuty events. If no default is configured, each PagerDuty action must specify a `description`.

    `incident_key`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A string that contains the default incident key to use when sending PagerDuty events.

    `client`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A string that specifies the default monitoring client.

    `client_url`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The URL of the default monitoring client.

    `event_type`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The default event type. Valid values: `trigger`,`resolve`, `acknowledge`.

    `attach_payload`
    :   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Whether or not to provide the watch payload as context for the event by default. Valid values: `true`, `false`.

`xpack.notification.webhook.additional_token_enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   When set to `true`, {{es}} automatically sets a token which enables the bypassing of traffic filters for calls initiated by Watcher towards {{es}} or {{kib}}. The default is `false` and the feature is available starting with {{es}} version 8.7.1 and later.

    ::::{important}
    This setting only applies to the Watcher `webhook` action, not the `http` input action.
    ::::



