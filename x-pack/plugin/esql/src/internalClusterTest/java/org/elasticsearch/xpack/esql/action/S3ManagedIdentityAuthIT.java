/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import fixture.aws.AwsCredentialsUtils;
import fixture.s3.BlobEntry;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpHandler;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.nettycommons.NettyCommonsPlugin;
import org.elasticsearch.xpack.esql.datasource.s3.S3Configuration;
import org.elasticsearch.xpack.esql.datasource.s3.S3DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end regression guard for {@code auth=managed_identity} on S3 external data sources,
 * using the registered-dataset path ({@code FROM <dataset>}).
 *
 * <p>Proof comes in two layers:
 * <ol>
 *   <li><b>Credential origin</b>: a mock IMDS server is started in {@code @BeforeClass} and wired
 *       via the {@code aws.ec2MetadataServiceEndpoint} system property — the official AWS SDK v2
 *       override for IMDS. {@link S3HttpHandler} validates the SigV4 Authorization header and
 *       rejects all others with HTTP 403, so a successful query <em>requires</em>
 *       {@code InstanceProfileCredentialsProvider} to have used the mock IMDS.</li>
 *   <li><b>Row return</b>: the query must return rows, proving the full path from
 *       {@code auth=managed_identity} data-source registration → cluster-setting gate → S3 client
 *       construction → IMDS credential resolution → actual data read.</li>
 * </ol>
 *
 * <p>The mock IMDS serves whatever {@link #imdsAccessKey} holds at fetch time. The wrong-credential
 * sub-test sets a different key and registers the datasource with a distinct endpoint string
 * ({@code 127.0.0.1} vs {@code localhost}) to force a cache miss in {@code StorageProviderRegistry}
 * — the cache key includes the config map, so a different endpoint string means a fresh provider
 * is constructed, whose first IMDS fetch picks up the updated key.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@SuppressForbidden(reason = "uses HttpServer for local S3 fixture and System.setProperty for workload identity credential seeding")
@ThreadLeakFilters(filters = { S3ManagedIdentityAuthIT.AwsSdkThreadFilter.class })
public class S3ManagedIdentityAuthIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    static final String WORKLOAD_IDENTITY_ACCESS_KEY = "workload-identity-test-access-key";
    static final String WORKLOAD_IDENTITY_SECRET_KEY = "workload-identity-test-secret-key";
    static final String BUCKET = "test-workload-identity-bucket";
    static final String OBJECT_KEY = "data/rows.ndjson";
    static final String DATASOURCE_NAME = "managed_identity_s3";
    static final String DATASET_NAME = "managed_identity_rows";

    /** Captures the Authorization header from the most recent S3 request for assertion. */
    static final AtomicReference<String> lastAuthorizationHeader = new AtomicReference<>();

    /** Access key served by the mock IMDS; swap before registering a datasource to inject a different credential. */
    static final AtomicReference<String> imdsAccessKey = new AtomicReference<>(WORKLOAD_IDENTITY_ACCESS_KEY);

    private static HttpServer s3Server;
    private static int s3Port;
    private static HttpServer imdsServer;

    @BeforeClass
    public static void startS3Server() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler(BUCKET, S3ConsistencyModel.STRONG_MPUS);
        byte[] ndjson = "{\"id\": 1, \"name\": \"Alice\"}\n{\"id\": 2, \"name\": \"Bob\"}\n".getBytes(StandardCharsets.UTF_8);
        s3Handler.blobs().put("/" + BUCKET + "/" + OBJECT_KEY, new BlobEntry(new BytesArray(ndjson), "STANDARD"));

        // Validate SigV4 signatures: only requests signed with WORKLOAD_IDENTITY_ACCESS_KEY are accepted.
        // checkAuthorization sends the 403 response itself when auth fails, so we just return.
        var authPredicate = AwsCredentialsUtils.fixedAccessKey(WORKLOAD_IDENTITY_ACCESS_KEY, () -> "us-east-1", "s3");
        s3Server = HttpServer.create(new InetSocketAddress(0), 0);
        s3Server.createContext("/", exchange -> {
            lastAuthorizationHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
            if (AwsCredentialsUtils.checkAuthorization(authPredicate, exchange) == false) {
                return;  // checkAuthorization already wrote the 403 response
            }
            s3Handler.handle(exchange);
        });
        s3Server.start();
        s3Port = s3Server.getAddress().getPort();
    }

    @AfterClass
    public static void stopS3Server() {
        if (s3Server != null) {
            s3Server.stop(0);
            s3Server = null;
        }
    }

    @BeforeClass
    public static void startImdsServer() throws Exception {
        imdsServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        // IMDSv2 token endpoint — the SDK PUTs here first; return a dummy token.
        imdsServer.createContext("/latest/api/token", exchange -> {
            byte[] token = "test-imds-token".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, token.length);
            exchange.getResponseBody().write(token);
            exchange.close();
        });
        // Credentials endpoints — list role, then return credentials JSON.
        // Credentials endpoint — list role then return credentials JSON.
        imdsServer.createContext("/latest/meta-data/iam/security-credentials", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.endsWith("security-credentials") || path.endsWith("security-credentials/")) {
                byte[] role = "test-role\n".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, role.length);
                exchange.getResponseBody().write(role);
            } else {
                String expiration = Instant.now().plusSeconds(3600).toString();
                String json = "{\"Code\":\"Success\",\"LastUpdated\":\"2025-01-01T00:00:00Z\","
                    + "\"Type\":\"AWS-HMAC\",\"AccessKeyId\":\""
                    + imdsAccessKey.get()
                    + "\",\"SecretAccessKey\":\""
                    + WORKLOAD_IDENTITY_SECRET_KEY
                    + "\",\"Expiration\":\""
                    + expiration
                    + "\"}";
                byte[] body = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        imdsServer.start();
        // Set before cluster nodes are created so InstanceProfileCredentialsProvider
        // uses the mock from the very first credential resolution.
        System.setProperty("aws.ec2MetadataServiceEndpoint", "http://localhost:" + imdsServer.getAddress().getPort());
    }

    @AfterClass
    public static void stopImdsServer() {
        System.clearProperty("aws.ec2MetadataServiceEndpoint");
        if (imdsServer != null) {
            imdsServer.stop(0);
            imdsServer = null;
        }
    }

    @Before
    public void resetImdsKey() {
        imdsAccessKey.set(WORKLOAD_IDENTITY_ACCESS_KEY);
    }

    @After
    public void restoreImdsKey() {
        imdsAccessKey.set(WORKLOAD_IDENTITY_ACCESS_KEY);
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    @After
    public void cleanupRegistry() throws Exception {
        try {
            client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { DATASET_NAME }))
                .get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
        try {
            client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { DATASOURCE_NAME })
            ).get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(NettyCommonsPlugin.class);
        plugins.add(S3DataSourcePlugin.class);
        plugins.add(NdJsonDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * Re-enables extension loading so S3 and NDJSON plugins register their format readers and
     * storage providers via SPI. {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses this
     * to keep the IT base lean.
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Tests
    // --------------------------------------------------------------------------------------------

    /**
     * Core regression guard: registers an S3 data source with {@code auth=managed_identity}, registers
     * a dataset pointing at the fixture, queries via {@code FROM <dataset>}, and asserts:
     * (a) rows are returned, and (b) the S3 Authorization header contains the workload identity access key
     * — proving the credential came from the workload identity chain (system properties) rather than
     * anonymous access or explicit credentials.
     */
    public void testManagedIdentityAuthReadsRowsAndUsesWorkloadIdentityCredential() throws Exception {
        registerManagedIdentityDatasource();
        registerDataset();

        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + DATASET_NAME + " | STATS count = COUNT(*)"))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("auth=managed_identity FROM query must return rows from fixture", rows, hasSize(greaterThanOrEqualTo(1)));
        }

        String authHeader = lastAuthorizationHeader.get();
        assertThat("S3 request must carry an Authorization header", authHeader, notNullValue());
        assertThat(
            "Authorization header must contain the workload identity access key from mock IMDS",
            authHeader,
            containsString(WORKLOAD_IDENTITY_ACCESS_KEY)
        );
    }

    /**
     * Wrong-credential counter-test: the fixture rejects requests not signed with
     * {@link #WORKLOAD_IDENTITY_ACCESS_KEY}, proving the auth gate is actually enforced.
     *
     * <p>Sets a wrong key in {@link #imdsAccessKey} <em>before</em> registering the datasource
     * and uses {@code 127.0.0.1} instead of {@code localhost} as the endpoint, forcing a cache miss
     * in {@code StorageProviderRegistry}. The fresh provider's first IMDS fetch returns the wrong
     * key, the S3 request is signed with it, and the fixture rejects with HTTP 403.
     */
    public void testQueryFailsWhenWrongCredentialIsUsed() throws Exception {
        imdsAccessKey.set("wrong-key-that-fixture-rejects");
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    DATASOURCE_NAME,
                    "s3",
                    null,
                    new HashMap<>(Map.of("auth", "managed_identity", "region", "us-east-1", "endpoint", "http://127.0.0.1:" + s3Port))
                )
            )
        );
        registerDataset();

        lastAuthorizationHeader.set(null);
        expectThrows(Exception.class, () -> {
            try (var ignored = run(syncEsqlQueryRequest("FROM " + DATASET_NAME + " | STATS count = COUNT(*)"))) {
                fail("query must fail: fixture rejects requests not signed with WORKLOAD_IDENTITY_ACCESS_KEY");
            }
        });
        String authHeader = lastAuthorizationHeader.get();
        assertThat("S3 request must have reached the fixture", authHeader, notNullValue());
        assertThat(
            "Authorization header must contain the wrong key injected via mock IMDS",
            authHeader,
            containsString("wrong-key-that-fixture-rejects")
        );
    }

    /**
     * Verifies that {@code auth=managed_identity} is rejected at data-source registration time when
     * the cluster setting is disabled.
     */
    public void testManagedIdentityAuthRejectedWhenSettingDisabledAtValidation() {
        // managedIdentityEnabled defaults to () -> false
        var validator = new FileDataSourceValidator("s3", S3Configuration::fromMap, java.util.Set.of("s3", "s3a", "s3n"));
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "managed_identity", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.managed_identity.enabled"));
    }

    /**
     * Counter-proof: the same data is reachable with explicit credentials, confirming the fixture
     * and data are healthy. This ensures a test failure in the workload identity test is a credential-chain
     * bug, not a fixture or data problem.
     */
    public void testExplicitCredentialsAlsoWork() throws Exception {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    DATASOURCE_NAME,
                    "s3",
                    null,
                    new HashMap<>(
                        Map.of(
                            "access_key",
                            WORKLOAD_IDENTITY_ACCESS_KEY,
                            "secret_key",
                            WORKLOAD_IDENTITY_SECRET_KEY,
                            "region",
                            "us-east-1",
                            "endpoint",
                            "http://localhost:" + s3Port
                        )
                    )
                )
            )
        );
        registerDataset();

        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + DATASET_NAME + " | STATS count = COUNT(*)"))) {
            assertThat(getValuesList(response), hasSize(equalTo(1)));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private void registerManagedIdentityDatasource() throws Exception {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    DATASOURCE_NAME,
                    "s3",
                    null,
                    new HashMap<>(Map.of("auth", "managed_identity", "region", "us-east-1", "endpoint", "http://localhost:" + s3Port))
                )
            )
        );
    }

    private void registerDataset() throws Exception {
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    DATASET_NAME,
                    DATASOURCE_NAME,
                    "s3://" + BUCKET + "/" + OBJECT_KEY,
                    null,
                    new HashMap<>()
                )
            )
        );
    }

    /** Suppresses background threads started by the AWS SDK S3 client (idle-connection-reaper, etc.). */
    public static class AwsSdkThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("idle-connection-reaper")
                || t.getName().startsWith("sdk-async-response")
                || t.getName().startsWith("aws-java-sdk");
        }
    }
}
