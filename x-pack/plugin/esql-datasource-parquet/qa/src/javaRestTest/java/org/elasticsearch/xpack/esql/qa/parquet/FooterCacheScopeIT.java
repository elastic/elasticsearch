/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that the Parquet footer cache is scoped per storage configuration (endpoint +
 * credentials), so that two data sources with different settings never share cache entries.
 *
 * <ol>
 *   <li><b>Credential isolation</b>: a data source whose credential may list a bucket but not read
 *       its objects must be refused with an error, even when another data source with a read-capable
 *       credential has already cached the same object's footer.</li>
 *   <li><b>Endpoint isolation</b>: two data sources pointing at different S3 stores with the same
 *       bucket name and object key must each read from their own store.</li>
 *   <li><b>Same-store sharing</b> ({@link #testSameStorageSettingsReuseCachedFooter}): two data
 *       sources with identical settings must still share cached footers — that sharing is
 *       correct and desirable.</li>
 * </ol>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FooterCacheScopeIT extends ESRestTestCase {

    private static final String BUCKET = "fc-cache-scope-bucket";
    private static final String READER_KEY = "reader_key";
    private static final String LIST_ONLY_KEY = "list_only_key";
    private static final String SECRET_KEY = "test_secret_key";
    private static final String REGION = "us-east-1";
    private static final String ENC_ID = "test";

    // fixture1: single S3 store with two credential tiers (reader + list-only)
    private static final SelectiveAccessS3HttpFixture fixture1 = new SelectiveAccessS3HttpFixture(BUCKET, READER_KEY, LIST_ONLY_KEY);

    // fixture2: second S3 store, same bucket name and reader key, different objects.
    // listOnlyKey=null so all authenticated traffic is accepted as a reader.
    private static final SelectiveAccessS3HttpFixture fixture2 = new SelectiveAccessS3HttpFixture(BUCKET, READER_KEY, null);

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .keystore("cluster.state.encryption.password." + ENC_ID, "footer-cache-scope-enc-password")
        .keystore("cluster.state.encryption.active_password_id", ENC_ID)
        .environment("AWS_REGION", REGION)
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture1).around(fixture2).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void skipForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void seedFixtures() throws IOException {
        // Test 1 (credential bypass): one file on fixture1; reader_key can read it, list_only_key cannot.
        fixture1.seedBlob("scope/refused/part-1.parquet", parquetBytes("A", 100, 20));

        // Test 2 (endpoint confusion): same key on both stores, same length, different rows.
        byte[] xBytes = parquetBytes("X", 100, 20);
        byte[] yBytes = parquetBytes("Y", 100, 20);
        fixture1.seedBlob("scope/endpoint/part-1.parquet", xBytes);
        fixture2.seedBlob("scope/endpoint/part-1.parquet", yBytes);

        // Test 3 (control): same file used twice through two data sources with identical settings.
        fixture1.seedBlob("scope/control/part-1.parquet", parquetBytes("C", 100, 20));
    }

    // per-test tracking for cleanup
    private final List<String> datasetsToDelete = new ArrayList<>();
    private final List<String> datasourcesToDelete = new ArrayList<>();

    @After
    public void cleanup() {
        for (String name : datasetsToDelete) {
            deleteIgnoringErrors("/_query/dataset/" + name);
        }
        for (String name : datasourcesToDelete) {
            deleteIgnoringErrors("/_query/data_source/" + name);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Test 1: credential that cannot read objects must be refused
    // -----------------------------------------------------------------------------------------

    /**
     * A data source whose credential may list a bucket but not read its objects must receive an
     * error, even when a data source with full read access has already populated the footer cache
     * for the same object. The cache must not bypass the store's access control.
     */
    public void testCredentialThatCannotReadObjectsIsRefused() throws IOException {
        String resource = "s3://" + BUCKET + "/scope/refused/part-1.parquet";

        // Populate the footer cache via a full-read credential.
        putDataSource("reader_ds1", fixture1.getAddress(), READER_KEY);
        putDataset("reader_rows1", "reader_ds1", resource);
        assertThat(rowCount(runEsql("FROM reader_rows1 | SORT id | LIMIT 100")), equalTo(20));

        // A list-only credential must not receive rows — the store must be consulted and refuse the read.
        putDataSource("list_only_ds1", fixture1.getAddress(), LIST_ONLY_KEY);
        putDataset("list_only_rows1", "list_only_ds1", resource);

        ResponseException e = expectThrows(ResponseException.class, () -> runEsql("FROM list_only_rows1 | SORT id | LIMIT 100"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    // -----------------------------------------------------------------------------------------
    // Test 2: a store at a different endpoint must serve its own objects
    // -----------------------------------------------------------------------------------------

    /**
     * Two data sources pointing at different S3 endpoints — same bucket name, same object key,
     * same file length — must each return rows from their own store. The footer cache must be
     * scoped by endpoint so that one store's cached entry does not satisfy reads from another.
     */
    public void testStoreAtAnotherEndpointReadsItsOwnObject() throws IOException {
        String resource = "s3://" + BUCKET + "/scope/endpoint/part-1.parquet";

        putDataSource("store_x_ds", fixture1.getAddress(), READER_KEY);
        putDataset("store_x_rows", "store_x_ds", resource);
        assertThat(firstSecret(runEsql("FROM store_x_rows | SORT id | LIMIT 1")), equalTo("X-secret-100"));

        putDataSource("store_y_ds", fixture2.getAddress(), READER_KEY);
        putDataset("store_y_rows", "store_y_ds", resource);
        assertThat(firstSecret(runEsql("FROM store_y_rows | SORT id | LIMIT 1")), equalTo("Y-secret-100"));
    }

    // -----------------------------------------------------------------------------------------
    // Test 3: identical settings share cache entries
    // -----------------------------------------------------------------------------------------

    /**
     * Two data sources with identical endpoint and credentials must share footer cache entries.
     * This is the correct and desirable behaviour for reducing redundant I/O.
     */
    public void testSameStorageSettingsReuseCachedFooter() throws IOException {
        String resource = "s3://" + BUCKET + "/scope/control/part-1.parquet";

        // Two data sources with identical settings — same endpoint, same key.
        putDataSource("control_ds1", fixture1.getAddress(), READER_KEY);
        putDataset("control_rows1", "control_ds1", resource);

        putDataSource("control_ds2", fixture1.getAddress(), READER_KEY);
        putDataset("control_rows2", "control_ds2", resource);

        Map<String, Object> r1 = runEsql("FROM control_rows1 | SORT id | LIMIT 100");
        Map<String, Object> r2 = runEsql("FROM control_rows2 | SORT id | LIMIT 100");

        assertThat(rowCount(r1), equalTo(20));
        assertThat(rowCount(r2), equalTo(20));
        assertThat(firstSecret(r1), equalTo(firstSecret(r2)));
    }

    // -----------------------------------------------------------------------------------------
    // Parquet generation
    // -----------------------------------------------------------------------------------------

    /**
     * Generates a minimal, valid Parquet file with columns {@code id} (INT64) and {@code secret}
     * (UTF-8 string). Row {@code i} has {@code id = startId + i} and
     * {@code secret = label + "-secret-" + (startId + i)}.
     *
     * <p>All 20 rows fit comfortably within the 64 KiB footer-tail prefetch window, so the full
     * file is cached on the first read.
     */
    private static byte[] parquetBytes(String label, int startId, int count) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("secret")
            .named("schema");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = buildOutputFile(out);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new UncompressedOnly())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1 << 20)
                .withPageSize(65536)
                .build()
        ) {
            for (int i = 0; i < count; i++) {
                int id = startId + i;
                Group g = groupFactory.newGroup();
                g.add("id", (long) id);
                g.add("secret", label + "-secret-" + id);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static OutputFile buildOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return outputStream.size();
                    }

                    @Override
                    public void write(int b) {
                        outputStream.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        outputStream.write(b, off, len);
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

            @Override
            public String getPath() {
                return "memory://footer-cache-scope-test.parquet";
            }
        };
    }

    /**
     * Minimal {@link CompressionCodecFactory} that supports only {@link CompressionCodecName#UNCOMPRESSED},
     * avoiding any dependency on Hadoop codec classes.
     */
    private static class UncompressedOnly implements CompressionCodecFactory {

        @Override
        public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
            if (codecName != CompressionCodecName.UNCOMPRESSED) {
                throw new UnsupportedOperationException("UncompressedOnly does not support codec: " + codecName);
            }
            return new BytesInputCompressor() {
                @Override
                public BytesInput compress(BytesInput bytes) {
                    return bytes;
                }

                @Override
                public CompressionCodecName getCodecName() {
                    return CompressionCodecName.UNCOMPRESSED;
                }

                @Override
                public void release() {}
            };
        }

        @Override
        public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
            if (codecName != CompressionCodecName.UNCOMPRESSED) {
                throw new UnsupportedOperationException("UncompressedOnly does not support codec: " + codecName);
            }
            return new BytesInputDecompressor() {
                @Override
                public BytesInput decompress(BytesInput bytes, int decompressedSize) {
                    return bytes;
                }

                @Override
                public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) {
                    int savedLimit = input.limit();
                    input.limit(input.position() + compressedSize);
                    output.put(input);
                    input.limit(savedLimit);
                }

                @Override
                public void release() {}
            };
        }

        @Override
        public void release() {}
    }

    // -----------------------------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------------------------

    private void putDataSource(String name, String endpoint, String accessKey) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        req.setJsonEntity(Strings.format("""
            {"type":"s3","settings":{"access_key":"%s","secret_key":"%s","endpoint":"%s"}}""", accessKey, SECRET_KEY, endpoint));
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        datasourcesToDelete.add(name);
    }

    private void putDataset(String name, String dataSource, String resource) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        req.setJsonEntity(Strings.format("""
            {"data_source":"%s","resource":"%s","settings":{"region":"%s"}}""", dataSource, resource, REGION));
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        datasetsToDelete.add(name);
    }

    private Map<String, Object> runEsql(String query) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        req.setJsonEntity(Strings.format("""
            {"query":"%s"}""", query.replace("\"", "\\\"")));
        Response r = client().performRequest(req);
        return entityAsMap(r);
    }

    private void deleteIgnoringErrors(String path) {
        try {
            Request req = new Request("DELETE", path);
            req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
            client().performRequest(req);
        } catch (Exception ignored) {}
    }

    // -----------------------------------------------------------------------------------------
    // Response helpers
    // -----------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static int rowCount(Map<String, Object> result) {
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        return values == null ? 0 : values.size();
    }

    @SuppressWarnings("unchecked")
    private static String firstSecret(Map<String, Object> result) {
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        if (values == null || values.isEmpty()) {
            throw new AssertionError("no rows in result");
        }
        // secret is the second column (index 1)
        return (String) values.get(0).get(1);
    }
}
