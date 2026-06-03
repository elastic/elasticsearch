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

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.nettycommons.NettyCommonsPlugin;
import org.elasticsearch.xpack.esql.datasource.s3.S3DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
 * End-to-end regression guard for {@code auth=ambient} on S3 external data sources,
 * using the registered-dataset path ({@code FROM <dataset>}).
 *
 * <p>Proof comes in two layers:
 * <ol>
 *   <li><b>Credential origin</b>: the AWS SDK signs the S3 request with the access key that
 *       {@code SystemPropertyCredentialsProvider} resolved from the {@code aws.accessKeyId}
 *       system property. The {@link S3HttpHandler} fixture validates the SigV4 Authorization
 *       header and rejects all others with HTTP 403, so a successful query <em>requires</em>
 *       the ambient chain to have picked up the system-property credential.</li>
 *   <li><b>Row return</b>: the query must return rows, proving the full path from
 *       {@code auth=ambient} data-source registration → cluster-setting gate → S3 client
 *       construction → ambient credential resolution → actual data read.</li>
 * </ol>
 *
 * <p>Uses {@code SystemPropertyCredentialsProvider} (second in our explicit ambient chain)
 * rather than environment variables (first) because Java tests cannot set env vars, but
 * {@code System.setProperty()} is available and sufficient to exercise the chain.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@SuppressForbidden(reason = "uses HttpServer for local S3 fixture and System.setProperty for ambient credential seeding")
@ThreadLeakFilters(filters = { FileSourceAmbientAuthIT.AwsSdkThreadFilter.class })
public class FileSourceAmbientAuthIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    static final String AMBIENT_ACCESS_KEY = "ambient-test-access-key";
    static final String AMBIENT_SECRET_KEY = "ambient-test-secret-key";
    static final String BUCKET = "test-ambient-bucket";
    static final String OBJECT_KEY = "data/rows.ndjson";
    static final String DATASOURCE_NAME = "ambient_s3";
    static final String DATASET_NAME = "ambient_rows";

    /** Captures the Authorization header from the most recent S3 request for assertion. */
    static final AtomicReference<String> lastAuthorizationHeader = new AtomicReference<>();

    private static HttpServer s3Server;
    private static int s3Port;

    @BeforeClass
    public static void startS3Server() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler(BUCKET, S3ConsistencyModel.STRONG_MPUS);
        byte[] ndjson = "{\"id\": 1, \"name\": \"Alice\"}\n{\"id\": 2, \"name\": \"Bob\"}\n".getBytes(StandardCharsets.UTF_8);
        s3Handler.blobs().put("/" + BUCKET + "/" + OBJECT_KEY, new BlobEntry(new BytesArray(ndjson), "STANDARD"));

        // Validate SigV4 signatures: only requests signed with AMBIENT_ACCESS_KEY are accepted.
        // checkAuthorization sends the 403 response itself when auth fails, so we just return.
        var authPredicate = AwsCredentialsUtils.fixedAccessKey(AMBIENT_ACCESS_KEY, () -> "us-east-1", "s3");
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

    @Before
    public void seedAmbientCredentials() {
        // SystemPropertyCredentialsProvider reads these; they are the second provider
        // in the ambient chain after EnvironmentVariableCredentialsProvider.
        System.setProperty("aws.accessKeyId", AMBIENT_ACCESS_KEY);
        System.setProperty("aws.secretAccessKey", AMBIENT_SECRET_KEY);
    }

    @After
    public void clearAmbientCredentials() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
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
            .put(ExternalSourceSettings.AMBIENT_CREDENTIALS_ENABLED.getKey(), true)
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
     * Core regression guard: registers an S3 data source with {@code auth=ambient}, registers
     * a dataset pointing at the fixture, queries via {@code FROM <dataset>}, and asserts:
     * (a) rows are returned, and (b) the S3 Authorization header contains the ambient access key
     * — proving the credential came from the ambient chain (system properties) rather than
     * anonymous access or explicit credentials.
     */
    public void testAmbientAuthReadsRowsAndUsesAmbientCredential() throws Exception {
        registerAmbientDatasource();
        registerDataset();

        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + DATASET_NAME + " | STATS count = COUNT(*)"))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("auth=ambient FROM query must return rows from fixture", rows, hasSize(greaterThanOrEqualTo(1)));
        }

        String authHeader = lastAuthorizationHeader.get();
        assertThat("S3 request must carry an Authorization header", authHeader, notNullValue());
        assertThat(
            "Authorization header must contain the ambient access key resolved from system properties",
            authHeader,
            containsString(AMBIENT_ACCESS_KEY)
        );
    }

    /**
     * Verifies that {@code auth=ambient} is rejected at data-source registration time when
     * the cluster setting is disabled.
     */
    public void testAmbientAuthRejectedWhenSettingDisabledAtValidation() {
        var validator = new org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator(
            "s3",
            org.elasticsearch.xpack.esql.datasource.s3.S3Configuration::fromMap,
            java.util.Set.of("s3", "s3a", "s3n")
        );  // ambientEnabled defaults to () -> false
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "ambient", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.ambient_credentials.enabled"));
    }

    /**
     * Wrong-credential counter-test: the fixture rejects requests not signed with
     * {@link #AMBIENT_ACCESS_KEY}, proving the auth gate is actually enforced.
     */
    public void testQueryFailsWhenWrongCredentialIsUsed() throws Exception {
        registerAmbientDatasource();
        registerDataset();

        System.setProperty("aws.accessKeyId", "wrong-key-that-fixture-rejects");
        try {
            expectThrows(Exception.class, () -> {
                try (var ignored = run(syncEsqlQueryRequest("FROM " + DATASET_NAME + " | STATS count = COUNT(*)"))) {
                    fail("query must fail: fixture rejects requests not signed with AMBIENT_ACCESS_KEY");
                }
            });
        } finally {
            System.setProperty("aws.accessKeyId", AMBIENT_ACCESS_KEY);
        }
    }

    /**
     * Counter-proof: the same data is reachable with explicit credentials, confirming the fixture
     * and data are healthy. This ensures a test failure in the ambient test is a credential-chain
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
                            AMBIENT_ACCESS_KEY,
                            "secret_key",
                            AMBIENT_SECRET_KEY,
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

    private void registerAmbientDatasource() throws Exception {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    DATASOURCE_NAME,
                    "s3",
                    null,
                    new HashMap<>(Map.of("auth", "ambient", "region", "us-east-1", "endpoint", "http://localhost:" + s3Port))
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
