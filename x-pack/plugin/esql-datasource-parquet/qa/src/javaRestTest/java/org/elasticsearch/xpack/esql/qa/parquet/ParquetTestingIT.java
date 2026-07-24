/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.Build;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainCompressionCodecFactory;
import org.elasticsearch.xpack.esql.datasource.parquet.PlainParquetReadOptions;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.HttpDownloadRetry;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * Integration tests that validate ESQL's Parquet reader against ground-truth data generated at runtime
 * by parquet-mr's standard {@code GroupReadSupport} reader.
 * <p>
 * Each test downloads a parquet file from the
 * <a href="https://github.com/apache/parquet-testing">apache/parquet-testing</a> repository (pinned
 * to a specific commit), reads it with parquet-mr's row-oriented Group reader to produce ground truth,
 * then compares against the ESQL output from {@code EXTERNAL "<url>"}.
 * <p>
 * The parquet-mr Group reader is an independent code path from ESQL's optimized column reader, making
 * this a true cross-implementation validation.
 *
 * <h2>Excluded files</h2>
 * See {@link #GOOD_DATA_FILES} javadoc for the full list of exclusions and their reasons.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetTestingIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(ParquetTestingIT.class);

    private static final String HASH = "fa255dfacf58c8bab428b5d0117d188acc8ad03f";
    private static final String BASE_URL = "https://raw.githubusercontent.com/apache/parquet-testing/" + HASH;

    /** Single {@code http}-type data source every dataset in this suite binds to; created once, cleaned up at teardown. */
    private static final String HTTP_DATA_SOURCE = "parquet_http_ds";

    /**
     * Good data files from apache/parquet-testing that ESQL should be able to read.
     * <p>
     * Excluded from this list:
     * <ul>
     *   <li>Encrypted files ({@code *.parquet.encrypted}, {@code aes256/})</li>
     *   <li>Variant encodings ({@code variant/}, {@code shredded_variant/})</li>
     *   <li>Geospatial types ({@code geospatial/*.parquet})</li>
     *   <li>Non-parquet files ({@code bloom_filter.bin}, {@code bloom_filter.xxhash.bin})</li>
     *   <li>{@code large_string_map.brotli.parquet} -- 2GB+, too large for CI</li>
     *   <li>Non-standard compression ({@code hadoop_lz4_compressed*.parquet}, {@code non_hadoop_lz4_compressed.parquet})</li>
     *   <li>Intentionally corrupt CRC ({@code datapage_v1-corrupt-checksum.parquet},
     *       {@code rle-dict-uncompressed-corrupt-checksum.parquet})</li>
     *   <li>Malformed dictionary ({@code nation.dict-malformed.parquet})</li>
     *   <li>Edge cases ({@code overflow_i16_page_cnt.parquet}, {@code unknown-logical-type.parquet})</li>
     *   <li>Nested types with no readable columns ({@code null_list.parquet}, {@code nulls.snappy.parquet})</li>
     *   <li>Complex nested types ({@code nested_lists.snappy.parquet}, {@code nested_maps.snappy.parquet},
     *       {@code nested_structs.rust.parquet}, {@code list_columns.parquet}, {@code nonnullable.impala.parquet},
     *       {@code nullable.impala.parquet}, {@code repeated_no_annotation.parquet},
     *       {@code repeated_primitive_no_list.parquet}, {@code incorrect_map_schema.parquet},
     *       {@code old_list_structure.parquet}, {@code map_no_value.parquet})</li>
     * </ul>
     */
    private static final List<String> GOOD_DATA_FILES = List.of(
        "data/alltypes_dictionary.parquet",
        "data/alltypes_plain.parquet",
        "data/alltypes_plain.snappy.parquet",
        "data/alltypes_tiny_pages.parquet",
        "data/alltypes_tiny_pages_plain.parquet",
        "data/binary.parquet",
        "data/binary_truncated_min_max.parquet",
        "data/byte_array_decimal.parquet",
        "data/byte_stream_split.zstd.parquet",
        "data/byte_stream_split_extended.gzip.parquet",
        "data/column_chunk_key_value_metadata.parquet",
        "data/concatenated_gzip_members.parquet",
        "data/data_index_bloom_encoding_stats.parquet",
        "data/data_index_bloom_encoding_with_length.parquet",
        "data/datapage_v1-snappy-compressed-checksum.parquet",
        "data/datapage_v1-uncompressed-checksum.parquet",
        "data/datapage_v2.snappy.parquet",
        "data/datapage_v2_empty_datapage.snappy.parquet",
        "data/delta_binary_packed.parquet",
        "data/delta_byte_array.parquet",
        "data/delta_encoding_optional_column.parquet",
        "data/delta_encoding_required_column.parquet",
        "data/delta_length_byte_array.parquet",
        "data/dict-page-offset-zero.parquet",
        "data/fixed_length_byte_array.parquet",
        "data/fixed_length_decimal.parquet",
        "data/fixed_length_decimal_legacy.parquet",
        "data/float16_nonzeros_and_nans.parquet",
        "data/float16_zeros_and_nans.parquet",
        "data/int32_decimal.parquet",
        "data/int32_with_null_pages.parquet",
        "data/int64_decimal.parquet",
        "data/int96_from_spark.parquet",
        "data/lz4_raw_compressed.parquet",
        "data/lz4_raw_compressed_larger.parquet",
        "data/nan_in_stats.parquet",
        "data/page_v2_empty_compressed.parquet",
        "data/plain-dict-uncompressed-checksum.parquet",
        "data/rle-dict-snappy-checksum.parquet",
        "data/rle_boolean_encoding.parquet",
        "data/single_nan.parquet",
        "data/sort_columns.parquet"
    );

    private static final List<String> BAD_DATA_FILES = List.of(
        "bad_data/ARROW-GH-41317.parquet",
        "bad_data/ARROW-GH-41321.parquet",
        "bad_data/ARROW-GH-43605.parquet",
        "bad_data/ARROW-GH-45185.parquet",
        "bad_data/ARROW-GH-47662.parquet",
        "bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet",
        "bad_data/ARROW-RS-GH-6229-LEVELS.parquet",
        "bad_data/PARQUET-1481.parquet"
    );

    /**
     * Files where timestamp value comparison is skipped because INT96 timestamp
     * conversion is implementation-specific and ESQL may clamp or interpret extreme
     * values differently from parquet-mr.
     */
    private static final Set<String> SKIP_TIMESTAMP_VALUE_CHECK = Set.of("data/int96_from_spark.parquet");

    /**
     * Bad data files that ESQL reads successfully (200 OK) -- the corruption is not
     * detectable by or relevant to ESQL's reader.
     */
    private static final Set<String> BAD_DATA_READS_OK = Set.of(
        "bad_data/ARROW-GH-43605.parquet",
        "bad_data/ARROW-GH-45185.parquet",
        "bad_data/ARROW-RS-GH-6229-LEVELS.parquet"
    );

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.httpOnlyTestCluster();

    private static CloseableHttpClient httpClient;

    /** Datasets and {@code http} data sources are gated to snapshot builds today (same gate as {@code DataSourceCrudRestIT}). */
    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void initHttpClient() {
        httpClient = HttpClients.createDefault();
    }

    @AfterClass
    public static void closeHttpClient() throws IOException {
        if (httpClient != null) {
            httpClient.close();
            httpClient = null;
        }
    }

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Before
    public void registerHttpDataSource() throws IOException {
        DatasetRegistry.ensureDataSource(client(), HTTP_DATA_SOURCE, "http", Map.of());
    }

    private final String parquetFile;
    private final boolean isBadData;

    public ParquetTestingIT(String testName, String parquetFile, boolean isBadData) {
        this.parquetFile = parquetFile;
        this.isBadData = isBadData;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (String file : GOOD_DATA_FILES) {
            String name = file.replace("data/", "").replace(".parquet", "");
            params.add(new Object[] { name, file, false });
        }
        for (String file : BAD_DATA_FILES) {
            String name = "bad_" + file.replace("bad_data/", "").replace(".parquet", "");
            params.add(new Object[] { name, file, true });
        }
        return params;
    }

    public void testParquetFile() throws Exception {
        String url = BASE_URL + "/" + parquetFile;
        // Bind a dataset to the file's URL so the query reads it via FROM <dataset> (the http data source is
        // registered once in registerHttpDataSource). Format is inferred from the .parquet extension; datasets
        // do not take a format setting.
        String dataset = DatasetRegistry.sanitizeDatasetName("pq_", parquetFile);
        DatasetRegistry.ensureDataset(client(), dataset, HTTP_DATA_SOURCE, url, null);

        if (isBadData) {
            testBadData(dataset);
        } else {
            testGoodData(url, dataset);
        }
    }

    private void testGoodData(String url, String dataset) throws Exception {
        logger.info("Testing good data: {}", parquetFile);

        byte[] parquetBytes;
        try {
            parquetBytes = downloadFile(url);
        } catch (IOException e) {
            // raw.githubusercontent.com occasionally rate-limits (HTTP 429) the pinned-commit fixture under
            // CI load; that is an environmental condition, not a reader regression.
            assumeNoException("Unable to download parquet-testing fixture [" + url + "]", e);
            return;
        }
        GroundTruth groundTruth;
        try {
            groundTruth = readGroundTruth(parquetBytes);
        } catch (Exception e) {
            // parquet-mr can't read this file (e.g. empty snappy page in datapage_v2_empty_datapage.snappy)
            // but ESQL might handle it fine — verify ESQL doesn't error out
            logger.warn("Ground truth reader failed for {}: {} — verifying ESQL reads it OK", parquetFile, e.getMessage());
            String query = buildQuery(dataset, 100000);
            Map<String, Object> result;
            try {
                result = runEsqlSync(requestObjectBuilder().query(query), new AssertWarnings.NoWarnings(), null);
            } catch (IOException ioe) {
                throw skipIfTransientFailure(ioe, "querying");
            }
            assertNotNull("ESQL should read " + parquetFile + " despite parquet-mr failure", result.get("columns"));
            return;
        }

        if (groundTruth.supportedColumns.isEmpty()) {
            logger.info("Skipping {}: all columns unsupported", parquetFile);
            return;
        }

        String query = buildQuery(dataset, Math.max(groundTruth.numRows + 1, 100000));
        Map<String, Object> result;
        try {
            result = runEsqlSync(requestObjectBuilder().query(query), new AssertWarnings.NoWarnings(), null);
        } catch (IOException e) {
            throw skipIfTransientFailure(e, "querying");
        } catch (org.elasticsearch.xcontent.XContentParseException e) {
            // ESQL returned 200 but the response contains raw binary that isn't valid UTF-8/JSON.
            // This happens for files with raw BINARY/FIXED_LEN_BYTE_ARRAY columns without string annotation.
            logger.info("ESQL response for {} contains non-UTF-8 binary data (cannot parse JSON) — skipping value check", parquetFile);
            return;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, String>> resultColumns = (List<Map<String, String>>) result.get("columns");
        @SuppressWarnings("unchecked")
        List<List<Object>> resultValues = (List<List<Object>>) result.get("values");

        assertNotNull("Result should have columns", resultColumns);
        assertNotNull("Result should have values", resultValues);
        assertEquals("Row count mismatch for " + parquetFile, groundTruth.numRows, resultValues.size());

        Map<String, Integer> esqlColIndex = new HashMap<>();
        Map<String, String> esqlColType = new HashMap<>();
        for (int i = 0; i < resultColumns.size(); i++) {
            Map<String, String> col = resultColumns.get(i);
            esqlColIndex.put(col.get("name"), i);
            esqlColType.put(col.get("name"), col.get("type"));
        }

        // Verify schema: supported ground-truth columns should appear in ESQL results with compatible types
        for (ColumnMeta col : groundTruth.supportedColumns) {
            assertTrue("ESQL result missing column '" + col.name + "' from " + parquetFile, esqlColIndex.containsKey(col.name));
            String actualType = esqlColType.get(col.name);
            assertTrue(
                "Type mismatch for '" + col.name + "' in " + parquetFile + ": expected '" + col.esqlType + "' but got '" + actualType + "'",
                areTypesCompatible(col.esqlType, actualType)
            );
        }

        // Compare values row by row
        for (int rowIdx = 0; rowIdx < groundTruth.numRows; rowIdx++) {
            Map<String, Object> gtRow = groundTruth.rows.get(rowIdx);
            List<Object> esqlRow = resultValues.get(rowIdx);

            for (ColumnMeta col : groundTruth.supportedColumns) {
                if (!esqlColIndex.containsKey(col.name)) {
                    continue;
                }
                int colIdx = esqlColIndex.get(col.name);
                Object gtValue = gtRow.get(col.name);
                Object esqlValue = esqlRow.get(colIdx);
                assertValuesEqual(parquetFile, col.name, rowIdx, col.esqlType, gtValue, esqlValue);
            }
        }
    }

    private void testBadData(String dataset) throws Exception {
        String query = buildQuery(dataset, 10000);
        logger.info("Testing bad data: {}", parquetFile);

        if (BAD_DATA_READS_OK.contains(parquetFile)) {
            try {
                Map<String, Object> result = runEsqlSync(requestObjectBuilder().query(query), new AssertWarnings.NoWarnings(), null);
                assertNotNull("Expected " + parquetFile + " to read successfully (known readable)", result.get("columns"));
                logger.info("Confirmed: {} is readable by ESQL despite being in bad_data/", parquetFile);
            } catch (IOException ex) {
                skipIfTransientFailure(ex, "testing bad data");
                throw new AssertionError("File " + parquetFile + " is in BAD_DATA_READS_OK but returned error: " + ex.getMessage(), ex);
            }
            return;
        }

        // Not using expectThrows here: a transient external-host failure (client-side timeout, or a
        // server-side 503 after the cluster exhausts its own retry budget) must be told apart from the
        // expected 4xx client error *before* asserting on the status code below.
        ResponseException ex;
        try {
            runEsqlSync(requestObjectBuilder().query(query), new AssertWarnings.NoWarnings(), null);
            throw new AssertionError("Expected " + parquetFile + " to produce a 4xx error, but the query succeeded");
        } catch (ResponseException e) {
            ex = skipIfTransientFailure(e, "testing bad data");
        } catch (IOException e) {
            throw skipIfTransientFailure(e, "testing bad data");
        }
        int status = ex.getResponse().getStatusLine().getStatusCode();
        assertTrue(
            "Bad data file " + parquetFile + " should produce a 4xx error but got " + status + ": " + ex.getMessage(),
            status >= 400 && status < 500
        );
    }

    /**
     * Whether {@code failure} is an environmental symptom of {@code raw.githubusercontent.com}
     * throttling/slowness reaching the cluster's {@code http} data source read, rather than a query or
     * reader defect: either the REST client gave up waiting on a response (a bare transport-level
     * {@link IOException}, e.g. {@link java.net.SocketTimeoutException}, carrying no HTTP response), or
     * the cluster itself gave up after exhausting its own retry budget against the throttled/unavailable
     * host (surfaced as a {@code 503} -- see {@code ExternalUnavailableException#status()} in the ESQL
     * datasources retry layer). Mirrors the handling already applied to {@link #downloadFile} failures.
     */
    private static boolean isTransientExternalFailure(IOException failure) {
        if (failure instanceof ResponseException responseException) {
            return responseException.getResponse().getStatusLine().getStatusCode() == 503;
        }
        return true;
    }

    /**
     * Skips the test via {@code assumeNoException} if {@code failure} is a
     * {@linkplain #isTransientExternalFailure transient external-host failure} encountered while
     * {@code action} (e.g. {@code "querying"}); {@code assumeNoException} always throws, so this
     * method never returns normally in that case. Otherwise returns {@code failure} unchanged, so
     * callers can either {@code throw} it to propagate as-is, or assign it (the declared type is the
     * caller's exception type, e.g. {@link ResponseException}, so no cast is needed) to keep handling
     * it below -- centralizing the classify-and-skip logic that would otherwise be repeated at every
     * {@code runEsqlSync} call site in this class.
     */
    private <T extends IOException> T skipIfTransientFailure(T failure, String action) {
        if (isTransientExternalFailure(failure)) {
            assumeNoException("External host unavailable while " + action + " [" + parquetFile + "]", failure);
        }
        return failure;
    }

    // -- Ground truth generation using parquet-mr --

    private record ColumnMeta(String name, String esqlType) {}

    private record GroundTruth(List<ColumnMeta> supportedColumns, List<Map<String, Object>> rows, int numRows) {}

    private GroundTruth readGroundTruth(byte[] parquetBytes) throws IOException {
        InputFile inputFile = new ByteArrayInputFile(parquetBytes);
        List<ColumnMeta> supportedColumns = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();

        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile, options)) {
            MessageType schema = fileReader.getFileMetaData().getSchema();

            for (Type field : schema.getFields()) {
                String esqlType = mapParquetTypeToEsql(field);
                if (esqlType != null) {
                    supportedColumns.add(new ColumnMeta(field.getName(), esqlType));
                }
            }

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = fileReader.readNextRowGroup()) != null) {
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    Group group = recordReader.read();
                    Map<String, Object> row = new HashMap<>();
                    for (ColumnMeta col : supportedColumns) {
                        int fieldIndex = schema.getFieldIndex(col.name);
                        Type fieldType = schema.getType(fieldIndex);
                        Object value = extractValue(group, fieldIndex, fieldType, col.esqlType);
                        row.put(col.name, value);
                    }
                    rows.add(row);
                }
            }
        }

        return new GroundTruth(supportedColumns, rows, rows.size());
    }

    private static Object extractValue(Group group, int fieldIndex, Type fieldType, String esqlType) {
        int repetitionCount = group.getFieldRepetitionCount(fieldIndex);
        if (repetitionCount == 0) {
            return null;
        }

        if (fieldType.isPrimitive()) {
            return extractPrimitiveValue(group, fieldIndex, fieldType.asPrimitiveType(), esqlType);
        }

        // List types
        if (isListType(fieldType)) {
            return extractListValues(group, fieldIndex, fieldType.asGroupType());
        }

        return null;
    }

    private static Object extractPrimitiveValue(Group group, int fieldIndex, PrimitiveType primitiveType, String esqlType) {
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

        return switch (primitiveType.getPrimitiveTypeName()) {
            case INT32 -> extractInt32Value(group, fieldIndex, logicalType, esqlType);
            case INT64 -> extractInt64Value(group, fieldIndex, logicalType, esqlType);
            case INT96 -> extractInt96Value(group, fieldIndex);
            case FLOAT -> (double) group.getFloat(fieldIndex, 0);
            case DOUBLE -> group.getDouble(fieldIndex, 0);
            case BOOLEAN -> group.getBoolean(fieldIndex, 0);
            case BINARY -> extractBinaryValue(group, fieldIndex, logicalType);
            case FIXED_LEN_BYTE_ARRAY -> extractFixedBinaryValue(group, fieldIndex, logicalType);
        };
    }

    private static Object extractInt32Value(Group group, int fieldIndex, LogicalTypeAnnotation logicalType, String esqlType) {
        int raw = group.getInteger(fieldIndex, 0);
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation dec = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
            return raw / Math.pow(10, dec.getScale());
        }
        if ("long".equals(esqlType)) {
            return (long) raw;
        }
        return raw;
    }

    private static Object extractInt64Value(Group group, int fieldIndex, LogicalTypeAnnotation logicalType, String esqlType) {
        long raw = group.getLong(fieldIndex, 0);
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation dec) {
            return raw / Math.pow(10, dec.getScale());
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            long millis = switch (ts.getUnit()) {
                case MILLIS -> raw;
                case MICROS -> raw / 1000;
                case NANOS -> raw / 1_000_000;
            };
            return formatTimestamp(millis);
        }
        return raw;
    }

    /**
     * Converts a Parquet INT96 timestamp to an ISO-8601 string.
     * <p>
     * INT96 is 12 bytes in little-endian order: the first 8 bytes encode nanoseconds elapsed within
     * the day, and the last 4 bytes encode the Julian day number. The Unix epoch (1970-01-01)
     * corresponds to Julian day 2,440,588. This method subtracts that offset to obtain the epoch day,
     * converts to milliseconds, and adds the sub-day nanos (truncated to millis).
     * <p>
     * This mirrors the production conversion in
     * {@code ParquetFormatReader.readInt96TimestampColumn} and
     * {@code PageColumnReader}, which use the same constants
     * ({@code JULIAN_EPOCH_OFFSET = 2_440_588}, {@code MILLIS_PER_DAY}, {@code NANOS_PER_MILLI}).
     */
    private static Object extractInt96Value(Group group, int fieldIndex) {
        org.apache.parquet.io.api.Binary binary = group.getInt96(fieldIndex, 0);
        byte[] bytes = binary.getBytes();
        long nanos = ByteBuffer.wrap(bytes, 0, 8).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
        int julianDay = ByteBuffer.wrap(bytes, 8, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
        long millisFromEpoch = (julianDay - 2_440_588L) * TimeUnit.DAYS.toMillis(1) + nanos / 1_000_000;
        return formatTimestamp(millisFromEpoch);
    }

    private static Object extractBinaryValue(Group group, int fieldIndex, LogicalTypeAnnotation logicalType) {
        org.apache.parquet.io.api.Binary binary = group.getBinary(fieldIndex, 0);
        if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            return binary.toStringUsingUTF8();
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation dec) {
            java.math.BigDecimal bd = new java.math.BigDecimal(new java.math.BigInteger(binary.getBytes()), dec.getScale());
            return bd.doubleValue();
        }
        // Raw binary -- return as UTF-8 string (matching ESQL's behavior)
        byte[] bytes = binary.getBytes();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static Object extractFixedBinaryValue(Group group, int fieldIndex, LogicalTypeAnnotation logicalType) {
        org.apache.parquet.io.api.Binary binary = group.getBinary(fieldIndex, 0);
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation dec) {
            java.math.BigDecimal bd = new java.math.BigDecimal(new java.math.BigInteger(binary.getBytes()), dec.getScale());
            return bd.doubleValue();
        }
        if (logicalType instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            return float16ToDouble(binary.getBytes());
        }
        byte[] bytes = binary.getBytes();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static double float16ToDouble(byte[] bytes) {
        int bits = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8);
        int sign = (bits >> 15) & 0x1;
        int exponent = (bits >> 10) & 0x1F;
        int fraction = bits & 0x3FF;

        double result;
        if (exponent == 0) {
            result = Math.scalb((double) fraction, -24);
        } else if (exponent == 0x1F) {
            result = (fraction == 0) ? Double.POSITIVE_INFINITY : Double.NaN;
        } else {
            result = Math.scalb((double) (fraction + 1024), exponent - 25);
        }
        return (sign == 1) ? -result : result;
    }

    private static List<Object> extractListValues(Group group, int fieldIndex, GroupType listType) {
        Group listGroup = group.getGroup(fieldIndex, 0);
        Type elementType = listType.getType(0).asGroupType().getType(0);
        String elementEsqlType = elementType.isPrimitive() ? mapPrimitiveType(elementType.asPrimitiveType()) : null;

        List<Object> values = new ArrayList<>();
        int elementCount = listGroup.getFieldRepetitionCount(0);
        for (int i = 0; i < elementCount; i++) {
            Group elementGroup = listGroup.getGroup(0, i);
            if (elementGroup.getFieldRepetitionCount(0) == 0) {
                values.add(null);
            } else if (elementType.isPrimitive()) {
                values.add(extractPrimitiveValue(elementGroup, 0, elementType.asPrimitiveType(), elementEsqlType));
            } else {
                values.add(null);
            }
        }
        return values;
    }

    private static boolean isListType(Type type) {
        if (!type.isPrimitive()) {
            LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
            return annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
        }
        return false;
    }

    // -- Type mapping --

    private static String mapParquetTypeToEsql(Type type) {
        if (type.isPrimitive()) {
            return mapPrimitiveType(type.asPrimitiveType());
        }
        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            GroupType listGroup = type.asGroupType();
            if (listGroup.getFieldCount() > 0) {
                GroupType repeatedGroup = listGroup.getType(0).asGroupType();
                if (repeatedGroup.getFieldCount() > 0) {
                    Type elementType = repeatedGroup.getType(0);
                    return mapParquetTypeToEsql(elementType);
                }
            }
            return null;
        }
        // Struct, Map, etc. -- unsupported
        return null;
    }

    private static String mapPrimitiveType(PrimitiveType primitiveType) {
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return "double";
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            return "date";
        }
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return "date";
        }
        if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
            || logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
            return "keyword";
        }
        if (logicalType instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            return "double";
        }
        if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intType) {
            if (intType.getBitWidth() <= 32) {
                return intType.isSigned() ? "integer" : "long";
            }
            return intType.isSigned() ? "long" : "unsigned_long";
        }

        return switch (primitiveType.getPrimitiveTypeName()) {
            case INT32 -> "integer";
            case INT64 -> "long";
            case INT96 -> "date";
            case FLOAT, DOUBLE -> "double";
            case BOOLEAN -> "boolean";
            case BINARY, FIXED_LEN_BYTE_ARRAY -> "keyword";
        };
    }

    static boolean areTypesCompatible(String expected, String actual) {
        if (expected.equals(actual)) {
            return true;
        }
        if (("keyword".equals(expected) && "text".equals(actual)) || ("text".equals(expected) && "keyword".equals(actual))) {
            return true;
        }
        if ("long".equals(expected) && "unsigned_long".equals(actual)) {
            return true;
        }
        return false;
    }

    // -- Value comparison --

    private void assertValuesEqual(String file, String colName, int rowIdx, String esqlType, Object gtValue, Object esqlValue) {
        String ctx = file + " col='" + colName + "' row=" + rowIdx;

        if (gtValue == null && esqlValue == null) {
            return;
        }
        if (gtValue == null || esqlValue == null) {
            // NaN from ground truth may appear as null in ESQL
            if (isNaN(gtValue) || isNaN(esqlValue)) {
                return;
            }
            assertEquals("Null mismatch at " + ctx + ": gt=" + gtValue + " esql=" + esqlValue, gtValue, esqlValue);
            return;
        }

        // NaN check
        if (isNaN(gtValue)) {
            if (esqlValue instanceof Number n) {
                assertTrue("Expected NaN at " + ctx + " but got " + esqlValue, Double.isNaN(n.doubleValue()));
            }
            return;
        }

        // Timestamp comparison
        if ("date".equals(esqlType) && gtValue instanceof String gtStr) {
            if (SKIP_TIMESTAMP_VALUE_CHECK.contains(file)) {
                return;
            }
            if (esqlValue instanceof String esqlStr) {
                long gtMillis = java.time.Instant.parse(gtStr).toEpochMilli();
                long esqlMillis = java.time.Instant.parse(esqlStr).toEpochMilli();
                assertEquals("Timestamp mismatch at " + ctx, gtMillis, esqlMillis);
            }
            return;
        }

        // Numeric comparison
        if (gtValue instanceof Number gtNum && esqlValue instanceof Number esqlNum) {
            if ("double".equals(esqlType)) {
                double gtd = gtNum.doubleValue();
                double esqld = esqlNum.doubleValue();
                if (Double.isNaN(gtd) && Double.isNaN(esqld)) {
                    return;
                }
                double epsilon = Math.abs(gtd) * 1e-6 + 1e-15;
                assertEquals("Float mismatch at " + ctx, gtd, esqld, epsilon);
            } else if ("unsigned_long".equals(esqlType)) {
                // The ESQL response already holds the decoded unsigned value (the JSON output edge decodes
                // unsigned_long blocks), so compare it directly against the ground truth. Both sides are compared
                // as unsigned over the full [0, 2^64-1] range: the ground truth is the raw INT64 bits reinterpreted
                // as unsigned, and the response value may be a Long (<= Long.MAX_VALUE) or a BigInteger (above it).
                java.math.BigInteger gtUnsigned = new java.math.BigInteger(Long.toUnsignedString(gtNum.longValue()));
                java.math.BigInteger esqlUnsigned = new java.math.BigInteger(esqlNum.toString());
                assertEquals("Unsigned long mismatch at " + ctx, gtUnsigned, esqlUnsigned);
            } else {
                assertEquals("Integer mismatch at " + ctx, gtNum.longValue(), esqlNum.longValue());
            }
            return;
        }

        // Boolean comparison
        if (gtValue instanceof Boolean gtBool) {
            assertEquals("Boolean mismatch at " + ctx, gtBool, esqlValue);
            return;
        }

        // String comparison
        if (gtValue instanceof String gtStr && esqlValue instanceof String esqlStr) {
            assertEquals("String mismatch at " + ctx, gtStr, esqlStr);
            return;
        }

        // List comparison
        if (gtValue instanceof List<?> gtList && esqlValue instanceof List<?> esqlList) {
            assertEquals("List size mismatch at " + ctx, gtList.size(), esqlList.size());
            for (int i = 0; i < gtList.size(); i++) {
                assertValuesEqual(file, colName + "[" + i + "]", rowIdx, esqlType, gtList.get(i), esqlList.get(i));
            }
            return;
        }

        // Fallback: string comparison
        assertEquals("Value mismatch at " + ctx, String.valueOf(gtValue), String.valueOf(esqlValue));
    }

    private static boolean isNaN(Object value) {
        if (value instanceof Double d) {
            return Double.isNaN(d);
        }
        if (value instanceof Float f) {
            return Float.isNaN(f);
        }
        return false;
    }

    // -- Utilities --

    private static String formatTimestamp(long epochMillis) {
        return java.time.Instant.ofEpochMilli(epochMillis).toString();
    }

    private static String buildQuery(String dataset, int limit) {
        return "FROM " + dataset + " | LIMIT " + limit;
    }

    private static final int DOWNLOAD_MAX_ATTEMPTS = 4;
    private static final long DOWNLOAD_INITIAL_BACKOFF_MILLIS = 1000L;
    private static final long DOWNLOAD_MAX_BACKOFF_MILLIS = 8000L;

    /**
     * Downloads {@code url}, retrying transient failures (HTTP 429/5xx, or connection-level errors below
     * the HTTP layer) via {@link HttpDownloadRetry#withRetries}. A permanent HTTP error (e.g. 404) is
     * thrown immediately without retrying. Throws the last failure once attempts are exhausted; the
     * caller treats that as an environmental skip rather than a test failure.
     */
    private static byte[] downloadFile(String url) throws IOException {
        return HttpDownloadRetry.withRetries(
            logger,
            "download [" + url + "]",
            DOWNLOAD_MAX_ATTEMPTS,
            DOWNLOAD_INITIAL_BACKOFF_MILLIS,
            DOWNLOAD_MAX_BACKOFF_MILLIS,
            () -> downloadFileOnce(url)
        );
    }

    private static byte[] downloadFileOnce(String url) throws IOException {
        HttpGet request = new HttpGet(url);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                throw new HttpDownloadRetry.HttpStatusException("Failed to download " + url + ": HTTP " + status, status);
            }
            return EntityUtils.toByteArray(response.getEntity());
        }
    }

    // -- Parquet-mr InputFile and Reader --

    /**
     * Minimal {@link InputFile} backed by a byte array for reading downloaded parquet files.
     */
    private static final class ByteArrayInputFile implements InputFile {
        private final byte[] data;

        ByteArrayInputFile(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new ByteArraySeekableInputStream(data);
        }
    }

    private static final class ByteArraySeekableInputStream extends SeekableInputStream {
        private final byte[] data;
        private int pos;

        ByteArraySeekableInputStream(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            this.pos = (int) newPos;
        }

        @Override
        public int read() {
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int available = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, available);
            pos += available;
            return available;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int offset, int length) throws IOException {
            if (pos + length > data.length) {
                throw new IOException("Not enough data: need " + length + " but only " + (data.length - pos) + " available");
            }
            System.arraycopy(data, pos, bytes, offset, length);
            pos += length;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            int length = buf.remaining();
            if (pos + length > data.length) {
                throw new IOException("Not enough data: need " + length + " but only " + (data.length - pos) + " available");
            }
            buf.put(data, pos, length);
            pos += length;
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            int length = buf.remaining();
            if (pos >= data.length) {
                return -1;
            }
            int available = Math.min(length, data.length - pos);
            buf.put(data, pos, available);
            pos += available;
            return available;
        }
    }
}
