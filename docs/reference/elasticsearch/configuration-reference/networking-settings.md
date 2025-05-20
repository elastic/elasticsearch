---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-network.html
applies_to:
  deployment:
    ess:
    self:
---

# Networking settings [modules-network]

Each {{es}} node has two different network interfaces. Clients send requests to {{es}}'s REST APIs using its [HTTP interface](#http-settings), but nodes communicate with other nodes using the [transport interface](#transport-settings). The transport interface is also used for communication with [remote clusters](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md). The transport interface uses a custom binary protocol sent over [long-lived](#long-lived-connections) TCP channels. Both interfaces can be configured to use [TLS for security](docs-content://deploy-manage/security.md).

You can configure both of these interfaces at the same time using the `network.*` settings. If you have a more complicated network, you might need to configure the interfaces independently using the `http.*` and `transport.*` settings. Where possible, use the `network.*` settings that apply to both interfaces to simplify your configuration and reduce duplication.

By default {{es}} binds only to `localhost` which means it cannot be accessed remotely. This configuration is sufficient for a local development cluster made of one or more nodes all running on the same host. To form a cluster across multiple hosts, or which is accessible to remote clients, you must adjust some [network settings](#common-network-settings) such as `network.host`.

::::{admonition} Be careful with the network configuration!
:class: warning

Never expose an unprotected node to the public internet. If you do, you are permitting anyone in the world to download, modify, or delete any of the data in your cluster.

::::


Configuring {{es}} to bind to a non-local address will [convert some warnings into fatal exceptions](docs-content://deploy-manage/deploy/self-managed/important-system-configuration.md#dev-vs-prod). If a node refuses to start after configuring its network settings then you must address the logged exceptions before proceeding.

## Commonly used network settings [common-network-settings]

Most users will need to configure only the following network settings.

`network.host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Sets the address of this node for both HTTP and transport traffic. The node will bind to this address and will also use it as its publish address. Accepts an IP address, a hostname, or a [special value](#network-interface-values).

    Defaults to `_local_`. However, note that [security auto-configuration](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md) will add `http.host: 0.0.0.0` to your `elasticsearch.yml` configuration file, which overrides this default for HTTP traffic.


`http.port`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The port to bind for HTTP client communication. Accepts a single value or a range. If a range is specified, the node will bind to the first available port in the range.

    Defaults to `9200-9300`.


`transport.port`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The port to bind for communication between nodes. Accepts a single value or a range. If a range is specified, the node will bind to the first available port in the range. Set this setting to a single port, not a range, on every master-eligible node.

    Defaults to `9300-9400`.


$$$remote_cluster.port$$$

`remote_cluster.port`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The port to bind for remote cluster client communication. Accepts a single value.

    Defaults to `9443`.



## Special values for network addresses [network-interface-values]

You can configure {{es}} to automatically determine its addresses by using the following special values. Use these values when configuring `network.host`, `network.bind_host`, `network.publish_host`, and the corresponding settings for the HTTP and transport interfaces.

`_local_`
:   Any loopback addresses on the system, for example `127.0.0.1`.

`_site_`
:   Any site-local addresses on the system, for example `192.168.0.1`.

`_global_`
:   Any globally-scoped addresses on the system, for example `8.8.8.8`.

`_[networkInterface]_`
:   Use the addresses of the network interface called `[networkInterface]`. For example if you wish to use the addresses of an interface called `en0` then set `network.host: _en0_`.

`0.0.0.0`
:   The addresses of all available network interfaces.

::::{note}
In some systems these special values resolve to multiple addresses. If so, {{es}} will select one of them as its publish address and may change its selection on each node restart. Ensure your node is accessible at every possible address.
::::


::::{note}
Any values containing a `:` (e.g. an IPv6 address or some of the [special values](#network-interface-values)) must be quoted because `:` is a special character in YAML.
::::


### IPv4 vs IPv6 [network-interface-values-ipv4-vs-ipv6]

These special values yield both IPv4 and IPv6 addresses by default, but you can also add an `:ipv4` or `:ipv6` suffix to limit them to just IPv4 or IPv6 addresses respectively. For example, `network.host: "_en0:ipv4_"` would set this node’s addresses to the IPv4 addresses of interface `en0`.

::::{admonition} Discovery in the Cloud
:class: tip

More special settings are available when running in the Cloud with either the [EC2 discovery plugin](/reference/elasticsearch-plugins/discovery-ec2.md) or the [Google Compute Engine discovery plugin](/reference/elasticsearch-plugins/discovery-gce-network-host.md) installed.

::::




## Binding and publishing [modules-network-binding-publishing]

{{es}} uses network addresses for two distinct purposes known as binding and publishing. Most nodes will use the same address for everything, but more complicated setups may need to configure different addresses for different purposes.

When an application such as {{es}} wishes to receive network communications, it must indicate to the operating system the address or addresses whose traffic it should receive. This is known as *binding* to those addresses. {{es}} can bind to more than one address if needed, but most nodes only bind to a single address. {{es}} can only bind to an address if it is running on a host that has a network interface with that address. If necessary, you can configure the transport and HTTP interfaces to bind to different addresses.

Each {{es}} node has an address at which clients and other nodes can contact it, known as its *publish address*. Each node has one publish address for its HTTP interface and one for its transport interface. These two addresses can be anything, and don’t need to be addresses of the network interfaces on the host. The only requirements are that each node must be:

* Accessible at its HTTP publish address by all clients that will discover it using sniffing.
* Accessible at its transport publish address by all other nodes in its cluster, and by any remote clusters that will discover it using [sniff mode](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md#sniff-mode).

Each node must have its own distinct publish address.

If you specify the transport publish address using a hostname then {{es}} will resolve this hostname to an IP address once during startup, and other nodes will use the resulting IP address instead of resolving the name again themselves. You must use a hostname such that all of the addresses to which it resolves are addresses at which the node is accessible from all other nodes. To avoid confusion, it is simplest to use a hostname which resolves to a single address.

If you specify the transport publish address using a [special value](#network-interface-values) then {{es}} will resolve this value to a single IP address during startup, and other nodes will use the resulting IP address instead of resolving the value again themselves. You must use a value such that all of the addresses to which it resolves are addresses at which the node is accessible from all other nodes. To avoid confusion, it is simplest to use a value which resolves to a single address. It is usually a mistake to use `0.0.0.0` as a publish address on hosts with more than one network interface.

### Using a single address [_using_a_single_address]

The most common configuration is for {{es}} to bind to a single address at which it is accessible to clients and other nodes. To use this configuration, set only `network.host` to the desired address. Do not separately set any bind or publish addresses. Do not separately specify the addresses for the HTTP or transport interfaces.


### Using multiple addresses [_using_multiple_addresses]

Use the [advanced network settings](#advanced-network-settings) if you wish to bind {{es}} to multiple addresses, or to publish a different address from the addresses to which you are binding. Set `network.bind_host` to the bind addresses, and `network.publish_host` to the address at which this node is exposed. In complex configurations, you can configure these addresses differently for the HTTP and transport interfaces.



## Advanced network settings [advanced-network-settings]

These advanced settings let you bind to multiple addresses, or to use different addresses for binding and publishing. They are not required in most cases and you should not use them if you can use the [commonly used settings](#common-network-settings) instead.

`network.bind_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address(es) to which the node should bind in order to listen for incoming connections. Accepts a list of IP addresses, hostnames, and [special values](#network-interface-values). Defaults to the address given by `network.host`. Use this setting only if binding to multiple addresses or using different addresses for publishing and binding.

`network.publish_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address that clients and other nodes can use to contact this node. Accepts an IP address, a hostname, or a [special value](#network-interface-values). Defaults to the address given by `network.host`. Use this setting only if binding to multiple addresses or using different addresses for publishing and binding.

::::{note}
You can specify a list of addresses for `network.host` and `network.publish_host`. You can also specify one or more hostnames or [special values](#network-interface-values) that resolve to multiple addresses. If you do this then {{es}} chooses one of the addresses for its publish address. This choice uses heuristics based on IPv4/IPv6 stack preference and reachability and may change when the node restarts. Ensure each node is accessible at all possible publish addresses.
::::


### Advanced TCP settings [tcp-settings]

Use the following settings to control the low-level parameters of the TCP connections used by the HTTP and transport interfaces.

`network.tcp.keep_alive`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_KEEPALIVE` option for network sockets, which determines whether each connection sends TCP keepalive probes. Defaults to `true`.

`network.tcp.keep_idle`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPIDLE` option for network sockets, which determines the time in seconds that a connection must be idle before starting to send TCP keepalive probes. Defaults to `-1`, which means to use the system default. This value cannot exceed `300` seconds. Only applicable on Linux and macOS.

`network.tcp.keep_interval`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPINTVL` option for network sockets, which determines the time in seconds between sending TCP keepalive probes. Defaults to `-1`, which means to use the system default. This value cannot exceed `300` seconds. Only applicable on Linux and macOS.

`network.tcp.keep_count`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPCNT` option for network sockets, which determines the number of unacknowledged TCP keepalive probes that may be sent on a connection before it is dropped. Defaults to `-1`, which means to use the system default. Only applicable on Linux and macOS.

`network.tcp.no_delay`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `TCP_NODELAY` option on network sockets, which determines whether [TCP no delay](https://en.wikipedia.org/wiki/Nagle%27s_algorithm) is enabled. Defaults to `true`.

`network.tcp.reuse_address`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_REUSEADDR` option for network sockets, which determines whether the address can be reused or not. Defaults to `false` on Windows and `true` otherwise.

`network.tcp.send_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Configures the size of the TCP send buffer for network sockets. Defaults to `-1` which means to use the system default.

`network.tcp.receive_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Configures the size of the TCP receive buffer. Defaults to `-1` which means to use the system default.



## Advanced HTTP settings [http-settings]

Use the following advanced settings to configure the HTTP interface independently of the [transport interface](#transport-settings). You can also configure both interfaces together using the [network settings](#common-network-settings).

`http.host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Sets the address of this node for HTTP traffic. The node will bind to this address and will also use it as its HTTP publish address. Accepts an IP address, a hostname, or a [special value](#network-interface-values). Use this setting only if you require different configurations for the transport and HTTP interfaces.

    Defaults to the address given by `network.host`. However, note that [security auto-configuration](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md) will add `http.host: 0.0.0.0` to your `elasticsearch.yml` configuration file, which overrides this default.


`http.bind_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address(es) to which the node should bind in order to listen for incoming HTTP connections. Accepts a list of IP addresses, hostnames, and [special values](#network-interface-values). Defaults to the address given by `http.host` or `network.bind_host`. Use this setting only if you require to bind to multiple addresses or to use different addresses for publishing and binding, and you also require different binding configurations for the transport and HTTP interfaces.

`http.publish_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address for HTTP clients to contact the node using sniffing. Accepts an IP address, a hostname, or a [special value](#network-interface-values). Defaults to the address given by `http.host` or `network.publish_host`. Use this setting only if you require to bind to multiple addresses or to use different addresses for publishing and binding, and you also require different binding configurations for the transport and HTTP interfaces.

`http.publish_port`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The port of the [HTTP publish address](#modules-network-binding-publishing). Configure this setting only if you need the publish port to be different from `http.port`. Defaults to the port assigned via `http.port`.

`http.max_content_length`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum size of an HTTP request body. If the body is compressed, the limit applies to the HTTP request body size before compression. Defaults to `100mb`. Configuring this setting to greater than `100mb` can cause cluster instability and is not recommended. If you hit this limit when sending a request to the [Bulk](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) API, configure your client to send fewer documents in each bulk request. If you wish to index individual documents that exceed `100mb`, pre-process them into smaller documents before sending them to {{es}}. For instance, store the raw data in a system outside {{es}} and include a link to the raw data in the documents that {{es}} indexes.

`http.max_initial_line_length`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum size of an HTTP URL. Defaults to `4kb`.

`http.max_header_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) Maximum size of allowed headers. Defaults to `16kb`.

$$$http-compression$$$

`http.compression` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Support for compression when possible (with Accept-Encoding). If HTTPS is enabled, defaults to `false`. Otherwise, defaults to `true`.

    Disabling compression for HTTPS mitigates potential security risks, such as a [BREACH attack](https://en.wikipedia.org/wiki/BREACH). To compress HTTPS traffic, you must explicitly set `http.compression` to `true`.


`http.compression_level`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Defines the compression level to use for HTTP responses. Valid values are in the range of 1 (minimum compression) and 9 (maximum compression). Defaults to `3`.

$$$http-cors-enabled$$$

`http.cors.enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Enable or disable cross-origin resource sharing, which determines whether a browser on another origin can execute requests against {{es}}. Set to `true` to enable {{es}} to process pre-flight [CORS](https://en.wikipedia.org/wiki/Cross-origin_resource_sharing) requests. {{es}} will respond to those requests with the `Access-Control-Allow-Origin` header if the `Origin` sent in the request is permitted by the `http.cors.allow-origin` list. Set to `false` (the default) to make {{es}} ignore the `Origin` request header, effectively disabling CORS requests because {{es}} will never respond with the `Access-Control-Allow-Origin` response header.

    ::::{note}
    If the client does not send a pre-flight request with an `Origin` header or it does not check the response headers from the server to validate the `Access-Control-Allow-Origin` response header, then cross-origin security is compromised. If CORS is not enabled on {{es}}, the only way for the client to know is to send a pre-flight request and realize the required response headers are missing.
    ::::


$$$http-cors-allow-origin$$$

`http.cors.allow-origin` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Which origins to allow. If you prepend and append a forward slash (`/`) to the value, this will be treated as a regular expression, allowing you to support HTTP and HTTPs. For example, using `/https?:\/\/localhost(:[0-9]+)?/` would return the request header appropriately in both cases. Defaults to no origins allowed.

    ::::{important}
    A wildcard (`*`) is a valid value but is considered a security risk, as your {{es}} instance is open to cross origin requests from **anywhere**.
    ::::


$$$http-cors-max-age$$$

`http.cors.max-age` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Browsers send a "preflight" OPTIONS-request to determine CORS settings. `max-age` defines for how long, in seconds, the result should be cached. Defaults to `1728000` (20 days).

$$$http-cors-allow-methods$$$

`http.cors.allow-methods` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Which methods to allow. Defaults to `OPTIONS, HEAD, GET, POST, PUT, DELETE`.

$$$http-cors-allow-headers$$$

`http.cors.allow-headers` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Which headers to allow. Defaults to `X-Requested-With, Content-Type, Content-Length, Authorization, Accept, User-Agent, X-Elastic-Client-Meta`.

$$$http-cors-expose-headers$$$

`http.cors.expose-headers` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Which response headers to expose in the client. Defaults to `X-elastic-product`.

$$$http-cors-allow-credentials$$$

`http.cors.allow-credentials` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Whether the `Access-Control-Allow-Credentials` header should be returned. Defaults to `false`.

    ::::{note}
    This header is only returned when the setting is set to `true`.
    ::::


`http.detailed_errors.enabled`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures whether detailed error reporting in HTTP responses is enabled. Defaults to `true`. When this option is set to `false`, only basic information is returned if an error occurs in the request, and requests with [`?error_trace` parameter](/reference/elasticsearch/rest-apis/common-options.md#common-options-error-options) set are rejected.

`http.pipelining.max_events`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The maximum number of events to be queued up in memory before an HTTP connection is closed, defaults to `10000`.

`http.max_warning_header_count`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The maximum number of warning headers in client HTTP responses. Defaults to `-1` which means the number of warning headers is unlimited.

`http.max_warning_header_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The maximum total size of warning headers in client HTTP responses. Defaults to `-1` which means the size of the warning headers is unlimited.

`http.tcp.keep_alive`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_KEEPALIVE` option for this socket, which determines whether it sends TCP keepalive probes. Defaults to `network.tcp.keep_alive`.

`http.tcp.keep_idle`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPIDLE` option for HTTP sockets, which determines the time in seconds that a connection must be idle before starting to send TCP keepalive probes. Defaults to `network.tcp.keep_idle`, which uses the system default. This value cannot exceed `300` seconds. Only applicable on Linux and macOS.

`http.tcp.keep_interval`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPINTVL` option for HTTP sockets, which determines the time in seconds between sending TCP keepalive probes. Defaults to `network.tcp.keep_interval`, which uses the system default. This value cannot exceed `300` seconds. Only applicable on Linux and macOS.

`http.tcp.keep_count`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPCNT` option for HTTP sockets, which determines the number of unacknowledged TCP keepalive probes that may be sent on a connection before it is dropped. Defaults to `network.tcp.keep_count`, which uses the system default. Only applicable on Linux and macOS.

`http.tcp.no_delay`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `TCP_NODELAY` option on HTTP sockets, which determines whether [TCP no delay](https://en.wikipedia.org/wiki/Nagle%27s_algorithm) is enabled. Defaults to `true`.

`http.tcp.reuse_address`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_REUSEADDR` option for HTTP sockets, which determines whether the address can be reused or not. Defaults to `false` on Windows and `true` otherwise.

`http.tcp.send_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The size of the TCP send buffer for HTTP traffic. Defaults to `network.tcp.send_buffer_size`.

`http.tcp.receive_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The size of the TCP receive buffer for HTTP traffic. Defaults to `network.tcp.receive_buffer_size`.

`http.client_stats.enabled`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), boolean) Enable or disable collection of HTTP client stats. Defaults to `true`.

`http.client_stats.closed_channels.max_count`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) When `http.client_stats.enabled` is `true`, sets the maximum number of closed HTTP channels for which {{es}} reports statistics. Defaults to `10000`.

`http.client_stats.closed_channels.max_age`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) When `http.client_stats.enabled` is `true`, sets the maximum length of time after closing a HTTP channel that {{es}} will report that channel’s statistics. Defaults to `5m`.

### HTTP client configuration [_http_client_configuration]

Many HTTP clients and proxies are configured for browser-like response latency and impose a fairly short timeout by default, reporting a failure if {{es}} takes longer than this timeout to complete the processing of a request. {{es}} will always eventually respond to every request, but some requests may require many minutes of processing time to complete. Consider carefully whether your client’s default response timeout is appropriate for your needs. In many cases it is better to wait longer for a response instead of failing, and this means you should disable any response timeouts:

* If you react to a timeout by retrying the request, the retry will often end up being placed at the back of the same queue which held the original request. It will therefore take longer to complete the processing of the request if you time out and retry instead of waiting more patiently. Retrying also imposes additional load on {{es}}.
* If a request is not idempotent and cannot be retried then failing the request is your last resort. Waiting more patiently for a response will usually allow the overall operation to succeed.

If you disable the response timeout in your client, make sure to configure TCP keepalives instead. TCP keepalives are the recommended way to prevent a client from waiting indefinitely in the event of a network outage.



## Advanced transport settings [transport-settings]

Use the following advanced settings to configure the transport interface independently of the [HTTP interface](#http-settings). Use the [network settings](#common-network-settings) to configure both interfaces together.

`transport.host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Sets the address of this node for transport traffic. The node will bind to this address and will also use it as its transport publish address. Accepts an IP address, a hostname, or a [special value](#network-interface-values). Use this setting only if you require different configurations for the transport and HTTP interfaces.

    Defaults to the address given by `network.host`.


`transport.bind_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address(es) to which the node should bind in order to listen for incoming transport connections. Accepts a list of IP addresses, hostnames, and [special values](#network-interface-values). Defaults to the address given by `transport.host` or `network.bind_host`. Use this setting only if you require to bind to multiple addresses or to use different addresses for publishing and binding, and you also require different binding configurations for the transport and HTTP interfaces.

`transport.publish_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address at which the node can be contacted by other nodes. Accepts an IP address, a hostname, or a [special value](#network-interface-values). Defaults to the address given by `transport.host` or `network.publish_host`. Use this setting only if you require to bind to multiple addresses or to use different addresses for publishing and binding, and you also require different binding configurations for the transport and HTTP interfaces.

`transport.publish_port`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The port of the [transport publish address](#modules-network-binding-publishing). Set this parameter only if you need the publish port to be different from `transport.port`. Defaults to the port assigned via `transport.port`.

`transport.connect_timeout`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) The connect timeout for initiating a new connection (in time setting format). Defaults to `30s`.

$$$transport-settings-compress$$$

`transport.compress` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Determines which transport requests are compressed before sending them to another node. {{es}} will compress transport responses if and only if the corresponding request was compressed. See also `transport.compression_scheme`, which specifies the compression scheme which is used. Accepts the following values:

    `false`
    :   No transport requests are compressed. This option uses the most network bandwidth, but avoids the CPU overhead of compression and decompression.

    `indexing_data`
    :   Compresses only the raw indexing data sent between nodes during ingest, CCR following (excluding bootstrapping) and operations-based shard recovery (excluding file-based recovery which copies the raw Lucene data). This option is a good trade-off between network bandwidth savings and the extra CPU required for compression and decompression. This option is the default.

    `true`
    :   All transport requests are compressed. This option may perform better than `indexing_data` in terms of network bandwidth, but will require the most CPU for compression and decompression work.


$$$transport-settings-compression-scheme$$$

`transport.compression_scheme` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Configures the compression scheme for requests which are selected for compression by to the `transport.compress` setting. Accepts either `deflate` or `lz4`, which offer different trade-offs between compression ratio and CPU usage. {{es}} will use the same compression scheme for responses as for the corresponding requests. Defaults to `lz4`.

`transport.tcp.keep_alive`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_KEEPALIVE` option for transport sockets, which determines whether they send TCP keepalive probes. Defaults to `network.tcp.keep_alive`.

`transport.tcp.keep_idle`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPIDLE` option for transport sockets, which determines the time in seconds that a connection must be idle before starting to send TCP keepalive probes. Defaults to `network.tcp.keep_idle` if set, or the system default otherwise. This value cannot exceed `300` seconds. In cases where the system default is higher than `300`, the value is automatically lowered to `300`. Only applicable on Linux and macOS.

`transport.tcp.keep_interval`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPINTVL` option for transport sockets, which determines the time in seconds between sending TCP keepalive probes. Defaults to `network.tcp.keep_interval` if set, or the system default otherwise. This value cannot exceed `300` seconds. In cases where the system default is higher than `300`, the value is automatically lowered to `300`. Only applicable on Linux and macOS.

`transport.tcp.keep_count`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPCNT` option for transport sockets, which determines the number of unacknowledged TCP keepalive probes that may be sent on a connection before it is dropped. Defaults to `network.tcp.keep_count` if set, or the system default otherwise. Only applicable on Linux and macOS.

`transport.tcp.no_delay`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `TCP_NODELAY` option on transport sockets, which determines whether [TCP no delay](https://en.wikipedia.org/wiki/Nagle%27s_algorithm) is enabled. Defaults to `true`.

`transport.tcp.reuse_address`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_REUSEADDR` option for network sockets, which determines whether the address can be reused or not. Defaults to `network.tcp.reuse_address`.

`transport.tcp.send_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The size of the TCP send buffer for transport traffic. Defaults to `network.tcp.send_buffer_size`.

`transport.tcp.receive_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The size of the TCP receive buffer for transport traffic. Defaults to `network.tcp.receive_buffer_size`.

`transport.ping_schedule`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Configures the time between sending application-level pings on all transport connections to promptly detect when a transport connection has failed. Defaults to `-1` meaning that application-level pings are not sent. You should use TCP keepalives (see `transport.tcp.keep_alive`) instead of application-level pings wherever possible.

### Transport profiles [transport-profiles]

Elasticsearch allows you to bind to multiple ports on different interfaces by the use of transport profiles. See this example configuration

```yaml
transport.profiles.default.port: 9300-9400
transport.profiles.default.bind_host: 10.0.0.1
transport.profiles.client.port: 9500-9600
transport.profiles.client.bind_host: 192.168.0.1
transport.profiles.dmz.port: 9700-9800
transport.profiles.dmz.bind_host: 172.16.1.2
```

The `default` profile is special. It is used as a fallback for any other profiles, if those do not have a specific configuration setting set, and is how this node connects to other nodes in the cluster. Other profiles can have any name and can be used to set up specific endpoints for incoming connections.

The following parameters can be configured on each transport profile, as in the example above:

* `port`: The port to which to bind.
* `bind_host`: The host to which to bind.
* `publish_host`: The host which is published in informational APIs.

Profiles also support all the other transport settings specified in the [transport settings](#transport-settings) section, and use these as defaults. For example, `transport.profiles.client.tcp.reuse_address` can be explicitly configured, and defaults otherwise to `transport.tcp.reuse_address`.


### Long-lived idle connections [long-lived-connections]

A transport connection between two nodes is made up of a number of long-lived TCP connections, some of which may be idle for an extended period of time. Nonetheless, {{es}} requires these connections to remain open, and it can disrupt the operation of your cluster if any inter-node connections are closed by an external influence such as a firewall. It is important to configure your network to preserve long-lived idle connections between {{es}} nodes, for instance by leaving `*.tcp.keep_alive` enabled and ensuring that the keepalive interval is shorter than any timeout that might cause idle connections to be closed, or by setting `transport.ping_schedule` if keepalives cannot be configured. Devices which drop connections when they reach a certain age are a common source of problems to {{es}} clusters, and must not be used.

If an {{es}} node is temporarily unable to handle network traffic it may stop reading data from the network and advertise a zero-length TCP window to its peers so that they pause the transmission of data to the unavailable node. This is the standard backpressure mechanism built into TCP. When the node becomes available again, it will resume reading from the network. Configure your network to permit TCP connections to exist in this paused state without disruption. Do not impose any limit on the length of time that a connection may remain in this paused state.

For information about troubleshooting unexpected network disconnections, see [Diagnosing other network disconnections](docs-content://troubleshoot/elasticsearch/troubleshooting-unstable-cluster.md#troubleshooting-unstable-cluster-network).


### Request compression [request-compression]

The default `transport.compress` configuration option `indexing_data` will only compress requests that relate to the transport of raw indexing source data between nodes. This option primarily compresses data sent during ingest, ccr, and shard recovery. This default normally makes sense for local cluster communication as compressing raw documents tends significantly reduce inter-node network usage with minimal CPU impact.

The `transport.compress` setting always configures local cluster request compression and is the fallback setting for remote cluster request compression. If you want to configure remote request compression differently than local request compression, you can set it on a per-remote cluster basis using the [`cluster.remote.${cluster_alias}.transport.compress` setting](docs-content://deploy-manage/remote-clusters/remote-clusters-settings.md).


### Response compression [response-compression]

The compression settings do not configure compression for responses. {{es}} will compress a response if the inbound request was compressed— even when compression is not enabled. Similarly, {{es}} will not compress a response if the inbound request was uncompressed— even when compression is enabled. The compression scheme used to compress a response will be the same scheme the remote node used to compress the request.



## Advanced remote cluster (API key based model) settings [remote-cluster-network-settings]

Use the following advanced settings to configure the remote cluster interface (API key based model) independently of the [transport interface](#transport-settings). You can also configure both interfaces together using the [network settings](#common-network-settings).

`remote_cluster_server.enabled`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Determines whether the remote cluster server should be enabled. This setting must be `true` for `remote_cluster.port` and all following remote cluster settings to take effect. Enabling it allows the cluster to serve cross-cluster requests using the API key based model. Defaults to `false`.

`remote_cluster.host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) Sets the address of this node for remote cluster server traffic. The node will bind to this address and will also use it as its remote cluster server publish address. Accepts an IP address, a hostname, or a [special value](#network-interface-values). Use this setting only if you require different configurations for the remote cluster server and transport interfaces.

    Defaults to the address given by `transport.bind_host`.


`remote_cluster.bind_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address(es) to which the node should bind in order to listen for incoming remote cluster connections. Accepts a list of IP addresses, hostnames, and [special values](#network-interface-values). Defaults to the address given by `remote_cluster.host`. Use this setting only if you require to bind to multiple addresses or to use different addresses for publishing and binding, and you also require different binding configurations for the remote cluster server and transport interfaces.

`remote_cluster.publish_host`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), string) The network address at which the node can be contacted by other nodes. Accepts an IP address, a hostname, or a [special value](#network-interface-values). Defaults to the address given by `remote_cluster.host`. Use this setting only if you require to bind to multiple addresses or to use different addresses for publishing and binding, and you also require different binding configurations for the remote cluster server and transport interfaces.

`remote_cluster.publish_port`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) The port of the [remote cluster server publish address](#modules-network-binding-publishing). Set this parameter only if you need the publish port to be different from `remote_cluster.port`. Defaults to the port assigned via `remote_cluster.port`.

`remote_cluster.tcp.keep_alive`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_KEEPALIVE` option for remote cluster sockets, which determines whether they send TCP keepalive probes. Defaults to `transport.tcp.keep_alive`.

`remote_cluster.tcp.keep_idle`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPIDLE` option for transport sockets, which determines the time in seconds that a connection must be idle before starting to send TCP keepalive probes. Defaults to `transport.tcp.keep_idle` if set, or the system default otherwise. This value cannot exceed `300` seconds. In cases where the system default is higher than `300`, the value is automatically lowered to `300`. Only applicable on Linux and macOS.

`remote_cluster.tcp.keep_interval`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPINTVL` option for transport sockets, which determines the time in seconds between sending TCP keepalive probes. Defaults to `transport.tcp.keep_interval` if set, or the system default otherwise. This value cannot exceed `300` seconds. In cases where the system default is higher than `300`, the value is automatically lowered to `300`. Only applicable on Linux and macOS.

`remote_cluster.tcp.keep_count`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Configures the `TCP_KEEPCNT` option for transport sockets, which determines the number of unacknowledged TCP keepalive probes that may be sent on a connection before it is dropped. Defaults to `transport.tcp.keep_count` if set, or the system default otherwise. Only applicable on Linux and macOS.

`remote_cluster.tcp.no_delay`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `TCP_NODELAY` option on transport sockets, which determines whether [TCP no delay](https://en.wikipedia.org/wiki/Nagle%27s_algorithm) is enabled. Defaults to `transport.tcp.no_delay`.

`remote_cluster.tcp.reuse_address`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), boolean) Configures the `SO_REUSEADDR` option for network sockets, which determines whether the address can be reused or not. Defaults to `transport.tcp.reuse_address`.

`remote_cluster.tcp.send_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The size of the TCP send buffer for transport traffic. Defaults to `transport.tcp.send_buffer_size`.

`remote_cluster.tcp.receive_buffer_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units)) The size of the TCP receive buffer for transport traffic. Defaults to `transport.tcp.receive_buffer_size`.


## Request tracing [_request_tracing]

You can trace individual requests made on the HTTP and transport layers.

::::{warning}
Tracing can generate extremely high log volumes that can destabilize your cluster. Do not enable request tracing on busy or important clusters.
::::


### REST request tracer [http-rest-request-tracer]

The HTTP layer has a dedicated tracer that logs incoming requests and the corresponding outgoing responses. Activate the tracer by setting the level of the `org.elasticsearch.http.HttpTracer` logger to `TRACE`:

```console
PUT _cluster/settings
{
   "persistent" : {
      "logger.org.elasticsearch.http.HttpTracer" : "TRACE"
   }
}
```

You can also control which URIs will be traced, using a set of include and exclude wildcard patterns. By default every request will be traced.

```console
PUT _cluster/settings
{
   "persistent" : {
      "http.tracer.include" : "*",
      "http.tracer.exclude" : ""
   }
}
```

By default, the tracer logs a summary of each request and response which matches these filters. To record the body of each request and response too, set the system property `es.insecure_network_trace_enabled` to `true`, and then set the levels of both the `org.elasticsearch.http.HttpTracer` and `org.elasticsearch.http.HttpBodyTracer` loggers to `TRACE`:

```console
PUT _cluster/settings
{
   "persistent" : {
      "logger.org.elasticsearch.http.HttpTracer" : "TRACE",
      "logger.org.elasticsearch.http.HttpBodyTracer" : "TRACE"
   }
}
```

Each message body is compressed, encoded, and split into chunks to avoid truncation:

```text
[TRACE][o.e.h.HttpBodyTracer     ] [master] [276] response body [part 1]: H4sIAAAAAAAA/9...
[TRACE][o.e.h.HttpBodyTracer     ] [master] [276] response body [part 2]: 2oJ93QyYLWWhcD...
[TRACE][o.e.h.HttpBodyTracer     ] [master] [276] response body (gzip compressed, base64-encoded, and split into 2 parts on preceding log lines)
```

Each chunk is annotated with an internal request ID (`[276]` in this example) which you should use to correlate the chunks with the corresponding summary lines. To reconstruct the output, base64-decode the data and decompress it using `gzip`. For instance, on Unix-like systems:

```sh
cat httptrace.log | sed -e 's/.*://' | base64 --decode | gzip --decompress
```

::::{warning}
HTTP request and response bodies may contain sensitive information such as credentials and keys, so HTTP body tracing is disabled by default. You must explicitly enable it on each node by setting the system property `es.insecure_network_trace_enabled` to `true`. This feature is primarily intended for test systems which do not contain any sensitive information. If you set this property on a system which contains sensitive information, you must protect your logs from unauthorized access.
::::



### Transport tracer [transport-tracer]

The transport layer has a dedicated tracer that logs incoming and outgoing requests and responses. Activate the tracer by setting the level of the `org.elasticsearch.transport.TransportService.tracer` logger to `TRACE`:

```console
PUT _cluster/settings
{
   "persistent" : {
      "logger.org.elasticsearch.transport.TransportService.tracer" : "TRACE"
   }
}
```

You can also control which actions will be traced, using a set of include and exclude wildcard patterns. By default every request will be traced except for fault detection pings:

```console
PUT _cluster/settings
{
   "persistent" : {
      "transport.tracer.include" : "*",
      "transport.tracer.exclude" : "internal:coordination/fault_detection/*"
   }
}
```



## Networking threading model [modules-network-threading-model]

This section describes the threading model used by the networking subsystem in {{es}}. This information isn’t required to use {{es}}, but it may be useful to advanced users who are diagnosing network problems in a cluster.

{{es}} nodes communicate over a collection of TCP channels that together form a transport connection. {{es}} clients communicate with the cluster over HTTP, which also uses one or more TCP channels. Each of these TCP channels is owned by exactly one of the `transport_worker` threads in the node. This owning thread is chosen when the channel is opened and remains the same for the lifetime of the channel.

Each `transport_worker` thread has sole responsibility for sending and receiving data over the channels it owns. Additionally, each http and transport server socket is assigned to one of the `transport_worker` threads. That worker has the responsibility of accepting new incoming connections to the server socket it owns.

If a thread in {{es}} wants to send data over a particular channel, it passes the data to the owning `transport_worker` thread for the actual transmission.

Normally the `transport_worker` threads will not completely handle the messages they receive. Instead, they will do a small amount of preliminary processing and then dispatch (hand off) the message to a different [threadpool](/reference/elasticsearch/configuration-reference/thread-pool-settings.md) for the rest of their handling. For instance, bulk messages are dispatched to the `write` threadpool, searches are dispatched to one of the `search` threadpools, and requests for statistics and other management tasks are mostly dispatched to the `management` threadpool. However in some cases the processing of a message is expected to be so quick that {{es}} will do all of the processing on the `transport_worker` thread rather than incur the overhead of dispatching it elsewhere.

By default, there is one `transport_worker` thread per CPU. In contrast, there may sometimes be tens-of-thousands of TCP channels. If data arrives on a TCP channel and its owning `transport_worker` thread is busy, the data isn’t processed until the thread finishes whatever it is doing. Similarly, outgoing data are not sent over a channel until the owning `transport_worker` thread is free. This means that we require every `transport_worker` thread to be idle frequently. An idle `transport_worker` looks something like this in a stack dump:

```text
"elasticsearch[instance-0000000004][transport_worker][T#1]" #32 daemon prio=5 os_prio=0 cpu=9645.94ms elapsed=501.63s tid=0x00007fb83b6307f0 nid=0x1c4 runnable  [0x00007fb7b8ffe000]
   java.lang.Thread.State: RUNNABLE
	at sun.nio.ch.EPoll.wait(java.base@17.0.2/Native Method)
	at sun.nio.ch.EPollSelectorImpl.doSelect(java.base@17.0.2/EPollSelectorImpl.java:118)
	at sun.nio.ch.SelectorImpl.lockAndDoSelect(java.base@17.0.2/SelectorImpl.java:129)
	- locked <0x00000000c443c518> (a sun.nio.ch.Util$2)
	- locked <0x00000000c38f7700> (a sun.nio.ch.EPollSelectorImpl)
	at sun.nio.ch.SelectorImpl.select(java.base@17.0.2/SelectorImpl.java:146)
	at io.netty.channel.nio.NioEventLoop.select(NioEventLoop.java:813)
	at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:460)
	at io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:986)
	at io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
	at java.lang.Thread.run(java.base@17.0.2/Thread.java:833)
```

In the [Nodes hot threads](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-hot-threads) API an idle `transport_worker` thread is reported like this:

```text
   0.0% [cpu=0.0%, idle=100.0%] (500ms out of 500ms) cpu usage by thread 'elasticsearch[instance-0000000004][transport_worker][T#1]'
     10/10 snapshots sharing following 9 elements
       java.base@17.0.2/sun.nio.ch.EPoll.wait(Native Method)
       java.base@17.0.2/sun.nio.ch.EPollSelectorImpl.doSelect(EPollSelectorImpl.java:118)
       java.base@17.0.2/sun.nio.ch.SelectorImpl.lockAndDoSelect(SelectorImpl.java:129)
       java.base@17.0.2/sun.nio.ch.SelectorImpl.select(SelectorImpl.java:146)
       io.netty.channel.nio.NioEventLoop.select(NioEventLoop.java:813)
       io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:460)
       io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:986)
       io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
       java.base@17.0.2/java.lang.Thread.run(Thread.java:833)
```

Note that `transport_worker` threads should always be in state `RUNNABLE`, even when waiting for input, because they block in the native `EPoll#wait` method. The `idle=` time reports the proportion of time the thread spent waiting for input, whereas the `cpu=` time reports the proportion of time the thread spent processing input it has received.

If a `transport_worker` thread is not frequently idle, it may build up a backlog of work. This can cause delays in processing messages on the channels that it owns. It’s hard to predict exactly which work will be delayed:

* There are many more channels than threads. If work related to one channel is causing delays to its worker thread, all other channels owned by that thread will also suffer delays.
* The mapping from TCP channels to worker threads is fixed but arbitrary. Each channel is assigned an owning thread in a round-robin fashion when the channel is opened. Each worker thread is responsible for many different kinds of channel.
* There are many channels open between each pair of nodes. For each request, {{es}} will choose from the appropriate channels in a round-robin fashion. Some requests may end up on a channel owned by a delayed worker while other identical requests will be sent on a channel that’s working smoothly.

If the backlog builds up too far, some messages may be delayed by many seconds. The node might even [fail its health checks](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/cluster-fault-detection.md) and be removed from the cluster. Sometimes, you can find evidence of busy `transport_worker` threads using the [Nodes hot threads](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-hot-threads) API. However, this API itself sends network messages so may not work correctly if the `transport_worker` threads are too busy. It is more reliable to use `jstack` to obtain stack dumps or use Java Flight Recorder to obtain a profiling trace. These tools are independent of any work the JVM is performing.

It may also be possible to identify some reasons for delays from the server logs. See for instance the following loggers:

`org.elasticsearch.transport.InboundHandler`
:   This logger reports a warning if processing an inbound message occupies a network thread for unreasonably long, which is almost certainly a bug. The warning includes some information which can be used to identify the message that took unreasonably long to process.

`org.elasticsearch.transport.OutboundHandler`
:   This logger reports a warning if sending an outbound message takes longer than expected. This duration includes time spent waiting for network congestion to clear, and time spent processing other work on the same network thread, so does not always indicate the presence of a bug related to the outbound message specified in the log entry.

`org.elasticsearch.common.network.ThreadWatchdog`
:   This logger reports a warning and a thread dump when it notices that a network thread has not made progress between two consecutive checks, which is almost certainly a bug:

    ```text
    [WARN ][o.e.c.n.ThreadWatchdog   ] the following threads are active but did not make progress in the preceding [5s]: [elasticsearch[instance-0000000004][transport_worker][T#1]]]
    [WARN ][o.e.c.n.ThreadWatchdog   ] hot threads dump due to active threads not making progress [part 1]: H4sIAAAAAAAA/+1aa2/bOBb93l8hYLUYFWgYvWw5AQbYpEkn6STZbJyiwAwGA1qiY8US6ZJUHvPr90qk/JJky41TtDMuUIci...
    [WARN ][o.e.c.n.ThreadWatchdog   ] hot threads dump due to active threads not making progress [part 2]: LfXL/x70a3eL8ve6Ral74ZBrp5x7HmUD9KXQz1MaXUNfFC6SeEysxSw1cNXL9JXYl3AigAE7ywbm/AZ+ll3Ox4qXJHNjVr6h...
    [WARN ][o.e.c.n.ThreadWatchdog   ] hot threads dump due to active threads not making progress (gzip compressed, base64-encoded, and split into 2 parts on preceding log lines; ...
    ```

    To reconstruct the thread dump, base64-decode the data and decompress it using `gzip`. For instance, on Unix-like systems:

    ```sh
    cat watchdog.log | sed -e 's/.*://' | base64 --decode | gzip --decompress
    ```

    This mechanism can be controlled with the following settings:

    `network.thread.watchdog.interval`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Defines the interval between watchdog checks. Defaults to `5s`. Set to `0` to disable the network thread watchdog.

    `network.thread.watchdog.quiet_time`
    :   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Defines the interval between watchdog warnings. Defaults to `10m`.



## TCP readiness port [tcp-readiness-port]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


If configured, a node can open a TCP port when the node is in a ready state. A node is deemed ready when it has successfully joined a cluster. In a single node configuration, the node is said to be ready, when it’s able to accept requests.

To enable the readiness TCP port, use the `readiness.port` setting. The readiness service will bind to all host addresses.

If the node leaves the cluster, or the [Shutdown API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-shutdown-put-node) is used to mark the node for shutdown, the readiness port is immediately closed.

A successful connection to the readiness TCP port signals that the {{es}} node is ready. When a client connects to the readiness port, the server simply terminates the socket connection. No data is sent back to the client. If a client cannot connect to the readiness port, the node is not ready.


