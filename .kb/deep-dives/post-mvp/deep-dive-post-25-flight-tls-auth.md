# Deep Dive: Item 25 -- Flight TLS + Auth

## Executive Summary

The Flight connector currently uses `Location.forGrpcInsecure()` everywhere -- in the factory, the connector, the split provider, and all tests. There is zero TLS support and zero authentication support. Making this production-ready requires (1) switching to TLS-enabled gRPC channels via Arrow Flight's `Location.forGrpcTls()` + `FlightClient.Builder.useTls()`, (2) wiring in Elasticsearch's existing `SSLService` infrastructure for certificate management, and (3) adding authentication support (bearer token headers as gRPC metadata). The Elasticsearch SSL infrastructure is mature and reusable -- the pattern from the Inference plugin is directly applicable.

---

## 1. Current Flight Connector Implementation

### 1.1 Plugin Structure

The Flight connector lives in `x-pack/plugin/esql-datasource-grpc/`. The plugin class:

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/GrpcDataSourcePlugin.java`

```java
public class GrpcDataSourcePlugin extends Plugin implements DataSourcePlugin {
    @Override
    public Set<String> supportedConnectorSchemes() {
        return Set.of("flight", "grpc");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of("flight", new FlightConnectorFactory());
    }
}
```

Key observation: The plugin receives `Settings settings` but does **not** use them. It does not receive or store `SSLService`. It does not register any SSL settings.

### 1.2 All Insecure Connection Points

There are **five** locations in production code using `Location.forGrpcInsecure()`:

**FlightConnectorFactory** (line 51) -- schema resolution:
```java
// File: .../grpc/FlightConnectorFactory.java:51
Location flightLocation = Location.forGrpcInsecure(uri.getHost(), port);
```

**FlightConnector constructor** (line 54) -- default client creation:
```java
// File: .../grpc/FlightConnector.java:54
Location location = Location.forGrpcInsecure(host, port);
```

**FlightConnector.clientForSplit** (line 107) -- on-demand client for multi-endpoint:
```java
// File: .../grpc/FlightConnector.java:107
Location location = Location.forGrpcInsecure(host, port);
```

**FlightSplitProvider.discoverSplits** (line 44) -- split discovery:
```java
// File: .../grpc/FlightSplitProvider.java:44
Location location = Location.forGrpcInsecure(uri.getHost(), port);
```

**EmployeeFlightServer** (line 90, test code) -- test server:
```java
// File: .../grpc/EmployeeFlightServer.java:90
this.location = Location.forGrpcInsecure("localhost", port);
```

### 1.3 gRPC Channel Setup

The channel is created implicitly through `FlightClient.builder(allocator, location).build()`. The Arrow Flight `FlightClient.Builder` class (in `flight-core-18.3.0.jar`) wraps gRPC's `ManagedChannelBuilder`. When the `Location` uses the `grpc+tcp://` scheme (which `forGrpcInsecure` produces), Flight uses `ManagedChannelBuilder.forAddress(host, port).usePlaintext()`.

For TLS, Arrow Flight provides:
- `Location.forGrpcTls(host, port)` -- creates a `grpc+tls://` Location
- `FlightClient.Builder.useTls()` -- forces TLS on the builder
- `FlightClient.Builder.channelBuilder(NettyChannelBuilder)` -- allows custom channel configuration with full control over SSL/TLS settings

### 1.4 Client Lifecycle Concerns

Each of the three production classes creates its own short-lived or long-lived clients:
- `FlightConnectorFactory.resolveMetadata()` -- creates a temporary client (try-with-resources), closes after schema fetch
- `FlightSplitProvider.discoverSplits()` -- creates a temporary client (try-with-resources), closes after split discovery
- `FlightConnector` -- creates a long-lived default client plus on-demand clients per split location (stored in `ConcurrentHashMap<String, FlightClient>`)

This means TLS configuration must be available at all three points. A centralized client factory would be cleaner.

---

## 2. RootAllocator with No Limit

Confirmed: `new RootAllocator()` is used with no argument in **four production code locations** and **multiple test locations**:

**Production code:**
1. `FlightConnectorFactory.java:54` -- `new RootAllocator()` in `resolveMetadata()`
2. `FlightConnector.java:55` -- `this.allocator = new RootAllocator()` in constructor
3. `FlightSplitProvider.java:46` -- `new RootAllocator()` in `discoverSplits()`

The `RootAllocator()` no-arg constructor in Apache Arrow (`arrow-memory-core-18.3.0.jar`) defaults to `Long.MAX_VALUE` as the memory limit. This means Arrow's off-heap memory allocations are completely unbounded -- there is no circuit breaker integration. This is a separate issue from TLS/auth but is relevant because the gRPC/Netty transport layer also allocates buffers through Arrow's allocator.

---

## 3. Elasticsearch TLS Infrastructure

### 3.1 SSLService

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/ssl/SSLService.java`

`SSLService` is the central TLS facility. Key capabilities:
- Loads SSL configurations from `elasticsearch.yml` settings (trust stores, key stores, PEM certificates)
- Creates and caches `SSLContext` instances per configuration
- Monitors certificate files for changes and reloads automatically
- Provides `SslProfile` objects with `sslContext()`, `engine(host, port)`, `socketFactory()`, etc.
- Supports named profiles like `xpack.security.transport.ssl`, `xpack.http.ssl`, `xpack.inference.elastic.http.ssl`

### 3.2 How Other Plugins Access SSLService

All X-Pack plugins access the shared instance:

```java
// Pattern used by Inference, Security, Watcher, Monitoring plugins:
protected SSLService getSslService() {
    return XPackPlugin.getSharedSslService();
}
```

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/XPackPlugin.java:236-242`

```java
public static SSLService getSharedSslService() {
    final SSLService ssl = XPackPlugin.sslService.get();
    if (ssl == null) {
        throw new IllegalStateException("SSL Service is not constructed yet");
    }
    return ssl;
}
```

### 3.3 SslProfileExtension (SPI for Registering New SSL Profiles)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/ssl/extension/SslProfileExtension.java`

```java
public interface SslProfileExtension {
    Set<String> getSettingPrefixes();        // e.g., {"xpack.esql.flight.ssl"}
    void applyProfile(String prefix, SslProfile profile);  // called after profile loaded
}
```

This is the mechanism for plugins to define new SSL configuration namespaces. When a plugin implements `SslProfileExtension`, ES automatically:
1. Registers `*.ssl.certificate`, `*.ssl.key`, `*.ssl.truststore`, etc. settings under the prefix
2. Loads and validates the SSL configuration on startup
3. Monitors certificate files for changes
4. Calls `applyProfile()` so the plugin can capture the `SslProfile`

### 3.4 SslProfile Interface

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/ssl/SslProfile.java`

```java
public interface SslProfile {
    SslConfiguration configuration();
    SSLContext sslContext();                    // <-- This is what gRPC needs
    SSLSocketFactory socketFactory();
    HostnameVerifier hostnameVerifier();
    SSLEngine engine(String host, int port);   // <-- Or this for per-connection engines
    void addReloadListener(Consumer<SslProfile> listener);  // <-- For cert rotation
}
```

### 3.5 Inference Plugin as Model

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/elastic/ElasticInferenceServiceSettings.java`

The Inference plugin registers an SSL profile with prefix `xpack.inference.elastic.http.ssl.`:

```java
public static final String ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX = "xpack.inference.elastic.http.ssl.";

public static final SSLConfigurationSettings ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_SETTINGS =
    SSLConfigurationSettings.withPrefix(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX, false);
```

Then in `HttpClientManager` (line 127-131):

```java
final SSLIOSessionStrategy sslioSessionStrategy = sslService
    .profile(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX)
    .ioSessionStrategy();
```

This is the exact pattern the Flight connector should follow.

---

## 4. Credential Storage Patterns for External Data Sources

### 4.1 S3 Credentials (Inline in Query)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-s3/src/main/java/org/elasticsearch/xpack/esql/datasource/s3/S3Configuration.java`

S3 credentials are currently passed inline as query parameters (not via ES keystore):

```java
public static S3Configuration fromParams(Map<String, Expression> params) {
    String accessKey = extractStringParam(params, "access_key");
    String secretKey = extractStringParam(params, "secret_key");
    String endpoint = extractStringParam(params, "endpoint");
    String region = extractStringParam(params, "region");
    ...
}
```

### 4.2 Azure Credentials (Multiple Auth Modes)

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-azure/src/main/java/org/elasticsearch/xpack/esql/datasource/azure/AzureConfiguration.java`

Azure supports multiple auth modes via inline config:

```java
public record AzureConfiguration(
    String connectionString,  // full connection string
    String account,           // storage account name
    String key,              // SharedKey auth
    String sasToken,         // SAS token
    String endpoint          // custom endpoint
) { ... }
```

### 4.3 No SecureSettings / Keystore Integration

Neither the S3 nor Azure data source plugins use `SecureSetting` or the ES keystore (`elasticsearch-keystore`). Credentials are passed through query parameters (`Map<String, Expression> params`) or resolved config maps (`Map<String, Object> config`). This means:
- No secure storage for data source credentials today
- The CRUD API (roadmap item) will need to address this for all connectors, including Flight

### 4.4 ConnectorFactory.open() Config Map

**File:** `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ConnectorFactory.java`

```java
public interface ConnectorFactory extends ExternalSourceFactory {
    Connector open(Map<String, Object> config);
}
```

The config map is a `Map<String, Object>`. Currently the Flight connector only passes `"endpoint"` and `"target"`. TLS and auth settings would need to be added here (or the interface would need to accept an SSL context / credentials object).

---

## 5. Arrow Flight Authentication Mechanisms

The Arrow Flight protocol (over gRPC) supports several authentication mechanisms:

### 5.1 Built-in Flight Auth (Handshake RPC)

Arrow Flight defines a `Handshake` RPC specifically for authentication. The client calls `authenticate(ClientAuthHandler)` which exchanges arbitrary bytes with the server. Common implementations:
- `ClientCookieMiddleware` -- cookie-based session auth
- `CredentialCallOption` -- username/password passed as gRPC metadata (deprecated in newer Arrow versions)

### 5.2 Bearer Token via gRPC Metadata

The most common production pattern is bearer token authentication via gRPC metadata headers:
- Client sends `Authorization: Bearer <token>` as a gRPC call option
- Arrow Flight's `FlightClient.Builder` supports `.intercept(ClientInterceptor)` for adding metadata
- gRPC's `MetadataUtils.newAttachHeadersInterceptor(metadata)` is the standard way

### 5.3 mTLS (Mutual TLS)

The client presents a certificate during TLS handshake. This is handled at the gRPC/Netty level, not at the Arrow Flight level. Requires configuring `SslContext` with client key material.

### 5.4 Available Dependencies

From `build.gradle` (line 63-67), the plugin already includes:
- `io.grpc:grpc-api:1.78.0` -- provides `Metadata`, `CallCredentials`, `ClientInterceptor`
- `io.grpc:grpc-core:1.78.0` -- provides `MetadataUtils`
- `io.grpc:grpc-netty:1.78.0` -- provides `NettySslContext` integration, `NettyChannelBuilder`
- `io.netty:netty-handler` -- provides `SslContext`, `SslContextBuilder`

These dependencies are sufficient for both TLS and bearer token auth. No additional libraries needed.

---

## 6. What the Integration Would Look Like

### 6.1 TLS Integration

**Step 1: Register an SSL profile prefix**

The `GrpcDataSourcePlugin` needs to implement `SslProfileExtension`:

```java
public class GrpcDataSourcePlugin extends Plugin implements DataSourcePlugin, SslProfileExtension {

    public static final String SSL_PREFIX = "xpack.esql.flight.ssl";

    private volatile SslProfile sslProfile;

    @Override
    public Set<String> getSettingPrefixes() {
        return Set.of(SSL_PREFIX);
    }

    @Override
    public void applyProfile(String prefix, SslProfile profile) {
        this.sslProfile = profile;
    }
}
```

Users would then configure in `elasticsearch.yml`:
```yaml
xpack.esql.flight.ssl.certificate_authorities: /path/to/ca.pem
xpack.esql.flight.ssl.verification_mode: full
```

**Step 2: Switch from insecure to TLS Locations**

Replace all `Location.forGrpcInsecure(host, port)` with `Location.forGrpcTls(host, port)` when TLS is configured. The scheme determines the gRPC channel type.

**Step 3: Wire SSLContext into FlightClient.Builder**

Arrow Flight's `FlightClient.Builder` supports custom channel builders:

```java
// Using grpc-netty's SslContext from ES's SSLService:
SSLContext jdkSslContext = sslProfile.sslContext();
SslContext nettySslContext = GrpcSslContexts.forClient()
    .trustManager(jdkSslContext.getTrustManagers()...)
    .build();

NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(host, port)
    .sslContext(nettySslContext);

FlightClient client = FlightClient.builder(allocator, location)
    .channelBuilder(channelBuilder)
    .build();
```

Alternatively, the simpler approach:
```java
FlightClient client = FlightClient.builder(allocator, Location.forGrpcTls(host, port))
    .useTls()
    .trustedCertificates(inputStreamFromSslProfile)
    .build();
```

### 6.2 Authentication Integration

**Bearer token approach** (simplest, most universal):

```java
Metadata headers = new Metadata();
headers.put(
    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
    "Bearer " + token
);
FlightClient client = FlightClient.builder(allocator, location)
    .intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
    .build();
```

The token would come from:
1. **Inline in query** (short-term): `EXTERNAL "flight://host/path" WITH (token="...")` -- matching S3/Azure pattern
2. **CRUD API** (medium-term): Named data source definition stored securely, resolved at query time
3. **ES Keystore** (secure): `xpack.esql.flight.auth.token` as a SecureSetting

### 6.3 Scheme-Based TLS Detection

A clean approach would be scheme-based:
- `flight://host:port/path` -- plaintext (development only)
- `flight+tls://host:port/path` or `grpcs://host:port/path` -- TLS required

This would require updating `FlightConnectorFactory.canHandle()`:
```java
// Current (line 40-41):
return location.startsWith("flight://") || location.startsWith("grpc://");
// Expanded:
return location.startsWith("flight://") || location.startsWith("grpc://")
    || location.startsWith("flight+tls://") || location.startsWith("grpcs://");
```

---

## 7. Complexity Assessment

### 7.1 TLS Only (no auth)

**Estimated complexity: Medium (1-2 weeks)**

What's needed:
1. Implement `SslProfileExtension` in `GrpcDataSourcePlugin` (~50 lines)
2. Add settings registration for `xpack.esql.flight.ssl.*` (~30 lines)
3. Create a `FlightClientFactory` helper that builds TLS or plaintext clients based on config (~100 lines)
4. Replace all 4 production `Location.forGrpcInsecure()` calls to use the factory (~40 lines changed)
5. Bridge ES `SSLContext` to Netty `SslContext` for gRPC (tricky -- ~50 lines, requires understanding the JDK-to-Netty SSL bridge)
6. Add TLS test infrastructure (test certificates, TLS-enabled test server) (~200 lines)
7. Integration test with TLS-enabled Flight server (~100 lines)

The hardest part is item 5 -- bridging JDK's `javax.net.ssl.SSLContext` (what ES `SSLService` provides) to Netty's `io.netty.handler.ssl.SslContext` (what gRPC-netty requires). There are two approaches:
- **JDK-based gRPC SslContext**: Use `io.grpc.netty.GrpcSslContexts.configure(SslContextBuilder.forClient())` with JDK SSL provider
- **Trust manager extraction**: Extract the `TrustManager` from ES's `SSLContext` and pass it to Netty's `SslContextBuilder.forClient().trustManager(trustManager)`

### 7.2 Bearer Token Auth

**Estimated complexity: Low (3-5 days)**

What's needed:
1. Add `token` to the config map in `FlightConnectorFactory.resolveMetadata()` (~10 lines)
2. Intercept gRPC calls with `Authorization` header (~20 lines)
3. Thread the token through `FlightConnector` and `FlightSplitProvider` (~30 lines)
4. Tests with an auth-requiring Flight server (~150 lines)

### 7.3 mTLS (Mutual TLS)

**Estimated complexity: Medium-Low (1 week, on top of TLS)**

Requires key material (client certificate + key) in addition to trust material. ES `SSLService` already supports this via `SslConfiguration.keyConfig()`. The gRPC `SslContext` builder accepts both trust and key material. Main work is testing.

### 7.4 Total Estimate

TLS + bearer token auth as a single work item: **~2-3 weeks H+AI** including tests and integration.

---

## 8. Risks and Considerations

### 8.1 JDK-to-Netty SSL Bridge

gRPC-netty uses Netty's SSL implementation, not JDK's. ES `SSLService` provides JDK `SSLContext`. The bridge requires careful handling:
- Option A: Use `SslContextBuilder.forClient().sslProvider(SslProvider.JDK)` -- uses JDK provider through Netty wrapper
- Option B: Extract `TrustManagerFactory` and `KeyManagerFactory` from the JDK context and pass to Netty's builder
- Option C: Use `GrpcSslContexts.forClient()` which auto-selects the best available provider (OpenSSL if available, JDK otherwise)

Option B is the most reliable since it works regardless of the SSL provider.

### 8.2 Certificate Reload

ES `SSLService` monitors certificate files and reloads them automatically. `SslProfile.addReloadListener()` allows the Flight plugin to be notified. However, existing `FlightClient` instances use a fixed gRPC channel -- they can't be reconfigured after creation. The reload listener would need to invalidate and recreate clients.

For `FlightConnector` (long-lived), this means closing and rebuilding the default client. The `ConcurrentHashMap<String, FlightClient>` already provides the infrastructure for this -- the map would need to be cleared on reload.

### 8.3 Per-Connection vs Global TLS

The current architecture uses a single SSL profile for all Flight connections (`xpack.esql.flight.ssl.*`). If different Flight servers require different certificates, per-connection TLS config would be needed. This could be deferred to the CRUD API (named data sources with individual TLS configs).

### 8.4 Test Infrastructure

The test Flight server (`EmployeeFlightServer`) currently uses `Location.forGrpcInsecure("localhost", port)` and `FlightServer.builder(allocator, location, producer).build()`. For TLS tests, it needs:
- Test certificates (self-signed CA + server cert)
- `FlightServer.builder().useTls(certChain, privateKey)`
- Trust configuration on the client side

The ES test framework already has certificate generation utilities (see `org.elasticsearch.test.ssl` and x-pack security test fixtures).

### 8.5 FIPS Compliance

ES supports FIPS 140-2 mode. The TLS implementation must use FIPS-approved algorithms. ES `SSLService` already handles this, but the JDK-to-Netty bridge must not bypass FIPS restrictions by using a non-FIPS SSL provider.

---

## 9. Summary of Findings

| Aspect | Current State | What Exists to Reuse | Gap |
|--------|--------------|---------------------|-----|
| TLS | Zero -- all `forGrpcInsecure()` | `SSLService`, `SslProfile`, `SslProfileExtension` | JDK-to-Netty SSL bridge, settings registration |
| Auth | Zero -- no credentials passed | gRPC interceptors, `Metadata` (in deps) | Token threading through config map |
| Credentials storage | N/A | S3/Azure inline pattern, CRUD API (roadmap) | No secure storage yet (shared problem) |
| Certificate reload | N/A | `SslProfile.addReloadListener()` | Client invalidation on reload |
| gRPC deps | `grpc-netty:1.78.0`, `netty-handler` already included | Full gRPC TLS stack available | No additional deps needed |
| Test infra | Plaintext only | ES test cert utilities | TLS-enabled test server |
| RootAllocator | `new RootAllocator()` = Long.MAX_VALUE limit | N/A | Unbounded -- separate issue, not blocking for TLS |

### Files That Need Changes

1. `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/GrpcDataSourcePlugin.java` -- implement `SslProfileExtension`, register settings, capture `SslProfile`
2. `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnectorFactory.java` -- accept SSL context, handle `flight+tls://` scheme, pass token
3. `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java` -- build TLS-enabled clients, intercept with auth headers
4. `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightSplitProvider.java` -- use TLS-enabled client for split discovery
5. `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/build.gradle` -- potentially add `compileOnly` on x-pack core for `SSLService` access
6. New file: `FlightClientFactory.java` -- centralized client creation with TLS/auth config
7. Test files: TLS-enabled `EmployeeFlightServer`, integration tests
