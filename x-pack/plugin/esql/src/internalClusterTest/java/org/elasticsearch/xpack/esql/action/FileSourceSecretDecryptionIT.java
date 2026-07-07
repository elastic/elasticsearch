/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Regression guard for the file-on-blob-storage read path: the {@link StorageProvider} factory must
 * receive the decrypted plaintext secret, not the {@link org.elasticsearch.xpack.encryption.spi.EncryptedData}
 * carrier it is stored as. {@code StorageProviderRegistry} decrypts at the single point every provider
 * client is built.
 *
 * <p>The proof comes in three layers:
 * <ul>
 *   <li>The capturing provider records the value handed to its factory for the {@code secret_token} key,
 *       and the test asserts it is the {@code String} the test {@code PutDataSource}'d.</li>
 *   <li>The provider's {@link CredentialGatedLocalStorageProvider} gate enforces value equality at byte-read
 *       time — every test sets {@code expectedCredentialOverride} to the value it will {@code PutDataSource},
 *       so a successful row-returning query <em>requires</em> the registry to have decrypted that exact
 *       String. Type-only matches no longer slip through.</li>
 *   <li>{@code testWrongCredentialFailsGracefully} drives the failure path: PUT one secret, configure the
 *       provider to expect another; the query must surface a graceful failure whose cause chain identifies
 *       a {@code credential mismatch} specifically (not just any "read denied").</li>
 * </ul>
 *
 * <p>Single-node by design, mirroring {@link FromDatasetIT}; multi-node dataset publication trips an
 * unrelated assertion on {@code main}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FileSourceSecretDecryptionIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String SECRET_KEY = "secret_token";
    private static final String SECRET_VALUE = "S3CR3T";
    private static final String SCHEME = "teststore";

    /** What the {@code teststore} provider factory observed for {@link #SECRET_KEY} at construction time. */
    static volatile Object capturedSecret;
    /** Whether the {@link #SECRET_KEY} key was even present in the config map handed to the factory. */
    static volatile boolean capturedSecretKeyPresent;
    /** Name of the thread that built the provider, logged for debugging. */
    static volatile String capturedConstructionThread;
    /** Makes each secret value unique so the provider cache never short-circuits a re-run. */
    private static final AtomicLong SECRET_SEQ = new AtomicLong();
    /**
     * If non-null, the storage provider built by the factory will only accept reads when {@link #capturedSecret}
     * equals this value — modelling a "wrong password" rejection. {@code null} means any plaintext String passes.
     */
    static volatile String expectedCredentialOverride;

    /**
     * {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses {@link ExtensiblePlugin#loadExtensions}
     * in the IT base; restore it so the datasource plugins register their format readers via SPI
     * (mirrors {@link ExternalSourceProfileIT}).
     */
    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    /**
     * Registers a capturing {@link StorageProvider} under the {@link #SCHEME} scheme. The provider
     * records the {@link #SECRET_KEY} value at factory time and serves bytes by reading the local
     * file named by the {@link StoragePath} (URI form {@code teststore:///<abs-local-path>}).
     *
     * <p>Storage-provider schemes are only registered for {@link DataSourcePlugin}s discovered via
     * SPI ({@code META-INF/services}) or ES extensions — not for plain node plugins — so this class
     * is listed in the internalClusterTest {@code META-INF/services} file rather than added to
     * {@link #nodePlugins()}. The {@code test} validator (which marks {@code secret_*} as secret) is
     * supplied by the SPI-registered {@code DataSourceCrudIT$TestDataSourcePlugin}, shared across ITs.
     */
    public static final class CapturingDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Set<String> supportedSchemes() {
            return Set.of(SCHEME);
        }

        @Override
        public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
            return Map.of(SCHEME, new CapturingStorageProviderFactory());
        }
    }

    private static final class CapturingStorageProviderFactory implements StorageProviderFactory {
        @Override
        public StorageProvider create(Settings settings) {
            // Zero-config path: no credential, so any subsequent byte read will throw via the provider's gate.
            return new CredentialGatedLocalStorageProvider(SCHEME, null, null);
        }

        @Override
        public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
            capturedSecretKeyPresent = config != null && config.containsKey(SECRET_KEY);
            capturedSecret = config == null ? null : config.get(SECRET_KEY);
            capturedConstructionThread = Thread.currentThread().getName();
            return new Configured<>(
                new CredentialGatedLocalStorageProvider(SCHEME, capturedSecret, expectedCredentialOverride),
                capturedSecretKeyPresent ? Set.of(SECRET_KEY) : Set.of()
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(NdJsonDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
        // CapturingDataSourcePlugin (teststore scheme) is registered via META-INF/services, not here.
        // The "test" validator comes from the SPI-shared DataSourceCrudIT$TestDataSourcePlugin.
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // Allow the system temp dir so file:// reads created by createTempDir() in each test are permitted.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void resetCapture() {
        capturedSecret = null;
        capturedSecretKeyPresent = false;
        capturedConstructionThread = null;
        expectedCredentialOverride = null;
    }

    @After
    public void cleanupRegistry() throws Exception {
        try {
            client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { "secret_ds" }))
                .get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
        try {
            client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { "secret_src" })
            ).get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    public void testCsvSecretIsDecryptedBeforeStorageProvider() throws Exception {
        Path fixture = createTempFile("secret-fixture-", ".csv");
        Files.writeString(fixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob") + "\n");
        runAndAssertSecretDecrypted(fixture, "csv");
    }

    public void testTsvSecretIsDecryptedBeforeStorageProvider() throws Exception {
        Path fixture = createTempFile("secret-fixture-", ".tsv");
        Files.writeString(fixture, String.join("\n", "emp_no:integer\tfirst_name:keyword", "1\tAlice", "2\tBob") + "\n");
        runAndAssertSecretDecrypted(fixture, "tsv");
    }

    public void testNdjsonSecretIsDecryptedBeforeStorageProvider() throws Exception {
        Path fixture = createTempFile("secret-fixture-", ".ndjson");
        Files.writeString(
            fixture,
            String.join("\n", "{\"emp_no\":1,\"first_name\":\"Alice\"}", "{\"emp_no\":2,\"first_name\":\"Bob\"}") + "\n"
        );
        runAndAssertSecretDecrypted(fixture, "ndjson");
    }

    public void testParquetSecretIsDecryptedBeforeStorageProvider() throws Exception {
        Path fixture = createTempDir().resolve("secret-fixture.parquet");
        writeParquetFile(fixture, 10);
        runAndAssertSecretDecrypted(fixture, "parquet");
    }

    private void runAndAssertSecretDecrypted(Path fixture, String format) throws Exception {
        // Unique per invocation: the StorageProviderRegistry caches providers by (scheme, config), so a
        // repeated value (across formats, or across re-runs of the same test under CI flakiness detection)
        // would hit the SUITE-scoped cache and skip createTrackingConsumedKeys, and the factory would never
        // observe the secret. A monotonic suffix keeps every run a cache miss.
        final String secretValue = SECRET_VALUE + "_" + format + "_" + SECRET_SEQ.incrementAndGet();
        // Tell the storage provider exactly what plaintext credential it should see at read time. The gate
        // does a full String comparison; the read fails if decryption produces anything other than this
        // exact value (e.g. wrong key, corrupted carrier, or the carrier left undecrypted entirely).
        expectedCredentialOverride = secretValue;
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "secret_src",
                    "test",
                    null,
                    new HashMap<>(Map.of(SECRET_KEY, secretValue))
                )
            )
        );
        String uri = SCHEME + "://" + StoragePath.fileUri(fixture).substring("file://".length());
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "secret_ds",
                    "secret_src",
                    uri,
                    null,
                    new HashMap<>(Map.of("format", format))
                )
            )
        );

        // End-to-end: the read must complete and return the fixture rows. If decryption succeeded but a
        // downstream step (storage byte-read, format decode, response wiring) broke, this assertion catches
        // it where a credential-only assertion would have silently passed.
        Exception queryFailure = null;
        try (
            org.elasticsearch.xpack.esql.action.EsqlQueryResponse response = run(
                syncEsqlQueryRequest("FROM secret_ds | KEEP emp_no, first_name | SORT emp_no | LIMIT 5"),
                TIMEOUT
            )
        ) {
            assertThat("[" + format + "] expected emp_no + first_name columns from the fixture", response.columns(), hasSize(2));
            assertThat(response.columns().get(0).name(), equalTo("emp_no"));
            assertThat(response.columns().get(1).name(), equalTo("first_name"));
            java.util.List<java.util.List<Object>> rows = getValuesList(response);
            assertThat("[" + format + "] expected at least one row from the fixture", rows, hasSize(greaterThanOrEqualTo(1)));
            assertThat(
                "[" + format + "] first row's emp_no must be the lowest int the fixture wrote",
                ((Number) rows.get(0).get(0)).intValue(),
                greaterThanOrEqualTo(0)
            );
            assertNotNull("[" + format + "] first row's first_name must not be null", rows.get(0).get(1));
        } catch (Exception e) {
            queryFailure = e;
        }

        Object captured = capturedSecret;
        logger.info(
            "[{}] captured secret class=[{}] keyPresent=[{}] constructionThread=[{}] value=[{}]",
            format,
            captured == null ? "null" : captured.getClass().getName(),
            capturedSecretKeyPresent,
            capturedConstructionThread,
            captured
        );

        assertTrue(
            "["
                + format
                + "] secret key was never seen by the storage provider factory (construction thread="
                + capturedConstructionThread
                + ", queryFailure="
                + queryFailure
                + ")",
            capturedSecretKeyPresent
        );
        assertNotNull("[" + format + "] storage provider factory captured a null secret value", captured);
        assertEquals(
            "["
                + format
                + "] storage provider must receive the DECRYPTED plaintext secret, but got a ["
                + captured.getClass().getName()
                + "] = ["
                + captured
                + "]. This proves the file path skips DataSourceCredentials.decryptInPlace.",
            String.class,
            captured.getClass()
        );
        assertEquals("[" + format + "] decrypted secret value", secretValue, captured);
        // End-to-end gate: the read had to actually complete. With the credential-gated provider,
        // this fails loudly if decryption produced anything other than the expected plaintext String.
        assertNull("[" + format + "] FROM query must complete without exception; got: " + queryFailure, queryFailure);
    }

    /**
     * Wrong-credential failure scenario: the storage provider is configured to require {@code RIGHT_PASSWORD}.
     * The data source's secret is {@code WRONG_PASSWORD}, which gets correctly encrypted at PUT and correctly
     * decrypted at read time — but the provider rejects it as a wrong-password mismatch. The query must fail
     * gracefully (a clean {@link Exception} surfaced to the client, no node crash), and the failure message
     * must point at the credential rejection so an operator can diagnose it.
     */
    public void testWrongCredentialFailsGracefully() throws Exception {
        expectedCredentialOverride = "RIGHT_PASSWORD_" + SECRET_SEQ.incrementAndGet();
        final String wrongSecret = "WRONG_PASSWORD_" + SECRET_SEQ.incrementAndGet();

        Path fixture = createTempFile("secret-fixture-wrong-", ".csv");
        Files.writeString(fixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob") + "\n");

        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "secret_src",
                    "test",
                    null,
                    new HashMap<>(Map.of(SECRET_KEY, wrongSecret))
                )
            )
        );
        String uri = SCHEME + "://" + StoragePath.fileUri(fixture).substring("file://".length());
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, "secret_ds", "secret_src", uri, null, new HashMap<>(Map.of("format", "csv")))
            )
        );

        Exception queryFailure = null;
        try (var ignored = run(syncEsqlQueryRequest("FROM secret_ds | LIMIT 5"), TIMEOUT)) {
            fail("expected the query to fail because the provider rejects the credential as wrong");
        } catch (Exception e) {
            queryFailure = e;
        }

        assertNotNull("expected a graceful failure surfaced to the client; got null", queryFailure);
        // The provider gate has three "read denied" branches: no-expected-configured, no-decrypted-plaintext
        // (decryption regression — credential is not a String), and credential-mismatch (the wrong-password
        // case this test models). Check for the *specific* mismatch marker so a regression in decryption
        // cannot make this test pass for the wrong reason.
        Throwable cur = queryFailure;
        boolean foundCredentialMismatch = false;
        while (cur != null) {
            if (cur.getMessage() != null && cur.getMessage().contains("credential mismatch")) {
                foundCredentialMismatch = true;
                break;
            }
            cur = cur.getCause();
        }
        assertTrue(
            "expected 'credential mismatch' (not just any 'read denied') in the cause chain; full toString=[" + queryFailure + "]",
            foundCredentialMismatch
        );
        // Decryption itself must still have succeeded: the captured value is the wrong (but plaintext) secret.
        assertEquals("provider must have observed the decrypted (wrong) secret", wrongSecret, capturedSecret);
    }

    private static void writeParquetFile(Path target, int rowCount) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test { required int64 emp_no; required binary first_name (UTF8); }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("emp_no", (long) i);
                g.add("first_name", "row_" + i);
                writer.write(g);
            }
        }
        Files.write(target, baos.toByteArray());
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }
}
