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
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Regression guard for the file-on-blob-storage read path: the {@link StorageProvider} factory must
 * receive the decrypted plaintext secret, not the {@link org.elasticsearch.xpack.encryption.spi.EncryptedData}
 * carrier it is stored as. {@code StorageProviderRegistry} decrypts at the single point every provider
 * client is built. The capturing provider records the value it observes for the {@code secret_token} key;
 * the test asserts it is the plaintext {@code String} across csv/tsv/ndjson/parquet.
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
            // Zero-config path: no secret to capture.
            return new CapturingStorageProvider();
        }

        @Override
        public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
            capturedSecretKeyPresent = config != null && config.containsKey(SECRET_KEY);
            capturedSecret = config == null ? null : config.get(SECRET_KEY);
            capturedConstructionThread = Thread.currentThread().getName();
            // Claim the secret key so the unknown-key validator does not reject it.
            return new Configured<>(new CapturingStorageProvider(), capturedSecretKeyPresent ? Set.of(SECRET_KEY) : Set.of());
        }
    }

    /** Serves bytes from the local file the {@code teststore} URI points at; delegates to a {@code file://} object. */
    private static final class CapturingStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new LocalDelegateObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new LocalDelegateObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new LocalDelegateObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            // Single-file fixtures only; no directory listing exercised by these tests.
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public StorageEntry next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public boolean exists(StoragePath path) {
            return Files.exists(localPathOf(path));
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of(SCHEME);
        }

        @Override
        public void close() {}
    }

    @SuppressForbidden(reason = "test converts a teststore:// URI to a local Path to serve fixture bytes")
    private static Path localPathOf(StoragePath path) {
        return PathUtils.get(path.localPath());
    }

    /** Reads the local file the {@code teststore} URI names. Minimal {@link StorageObject} surface. */
    private static final class LocalDelegateObject implements StorageObject {
        private final StoragePath path;
        private final Path file;

        LocalDelegateObject(StoragePath path) {
            this.path = path;
            this.file = localPathOf(path);
        }

        @Override
        public InputStream newStream() throws IOException {
            return Files.newInputStream(file);
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            InputStream in = Files.newInputStream(file);
            long skipped = 0;
            while (skipped < position) {
                long n = in.skip(position - skipped);
                if (n <= 0) {
                    break;
                }
                skipped += n;
            }
            return new BoundedInputStream(in, length);
        }

        @Override
        public long length() throws IOException {
            return Files.size(file);
        }

        @Override
        public Instant lastModified() throws IOException {
            return Files.getLastModifiedTime(file).toInstant();
        }

        @Override
        public boolean exists() {
            return Files.exists(file);
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

    /** Caps an InputStream to a byte length, for range reads. */
    private static final class BoundedInputStream extends InputStream {
        private final InputStream delegate;
        private long remaining;

        BoundedInputStream(InputStream delegate, long length) {
            this.delegate = delegate;
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = delegate.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int n = delegate.read(b, off, toRead);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
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

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void resetCapture() {
        capturedSecret = null;
        capturedSecretKeyPresent = false;
        capturedConstructionThread = null;
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
        // Use a per-format secret value so the StorageProviderRegistry's (scheme, config) provider
        // cache and the schema cache produce distinct keys per test. Otherwise a SUITE-scoped cache
        // hit from an earlier test would short-circuit createTrackingConsumedKeys and the factory
        // would never observe (or re-capture) the secret.
        final String secretValue = SECRET_VALUE + "_" + format;
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

        Exception queryFailure = null;
        try (var ignored = run(syncEsqlQueryRequest("FROM secret_ds | LIMIT 5"), TIMEOUT)) {
            // read may or may not complete depending on whether the gap throws downstream
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
