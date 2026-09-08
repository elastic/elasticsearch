/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Exhaustive acceptance harness for the datasource/dataset CRUD API.
 *
 * <p>Verifies the contract: a PUT that returns 200 must guarantee that a subsequent
 * {@code FROM <dataset> | LIMIT 1} query also succeeds. Every case is a full Cartesian-product
 * combination of dimension values per format, so cross-dimension interactions are caught
 * automatically — not just individual settings in isolation.
 *
 * <p>Dimensions include boundary and garbage values alongside valid ones. Some garbage values
 * (e.g. {@code ss_neg}, {@code em_garbage}) are already rejected at PUT by coordinator-level
 * validation and serve as canaries for that path. Others (e.g. {@code del_empty}, {@code del_multi})
 * slip through PUT — the reader treats them as the format default — and are exercised optimistically:
 * if PUT accepts them and the query then fails, CI catches the regression. Dimension values that are
 * entirely unreachable at query time are commented out with a reference to esql-planning#1550.
 *
 * <p>If a case fails (PUT returns 200 but the query fails due to a mis-wired or mis-validated
 * setting), mute it individually in {@code muted-tests.yml} by its case name with a reference to
 * the tracking issue. When the underlying fix lands, remove the mute entry.
 *
 * <p><b>Fixture note</b>: text-format fixtures (CSV, TSV, NDJSON) use a tiny {@code a,b} schema;
 * Parquet/ORC reuse {@code employees.*} from the standard test fixture set. The schemas differ, but
 * since the test only issues {@code LIMIT 1} no cross-format schema parity is required.
 *
 * <p><b>Encoding limitation</b>: all text fixture files contain only ASCII content. Encoding tests
 * (e.g. {@code encoding=ISO-8859-1}) therefore verify that the setting is accepted and plumbed
 * through to the reader, but do not exercise the actual charset-decoding path.
 *
 * <p>TODO: add encoding-specific fixture files containing bytes that are valid in the declared
 * charset but invalid UTF-8 (e.g. the copyright sign 0xA9 in ISO-8859-1). Assert that the
 * declared encoding yields the correct character and that omitting the declaration either errors
 * or produces the replacement character. Small binary files, one per charset under test.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class DataSourceCrudAcceptCombinationsIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(DataSourceCrudAcceptCombinationsIT.class);

    // Initialized before the @ClassRule cluster so files exist when esql.external.local_allowed_paths is evaluated.
    private static final Path FIXTURE_DIR = initFixtureDir();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(FIXTURE_DIR, config -> {}, false);

    // One shared local datasource for the whole class — cuts ~1800 REST calls vs per-case create/delete.
    // Datasets are per-case for isolation.
    private static final String SHARED_DS_NAME = "combo_shared_ds";

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    // DatasetRegistry.ensureDataSource is idempotent and only caches on success, so a transient
    // failure on the first test does not prevent retries on subsequent ones. Called in @Before rather
    // than @BeforeClass because client() is null during static class setup.
    @Before
    public void ensureSharedDs() throws IOException {
        DatasetRegistry.ensureDataSource(client(), SHARED_DS_NAME, "local", Map.of());
    }

    @AfterClass
    public static void teardownSharedDs() {
        if (Build.current().isSnapshot() == false) {
            return;
        }
        try {
            DatasetRegistry.cleanup(client());
        } catch (Exception e) {
            logger.warn("Failed to clean up shared datasource during teardown", e);
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    // This suite creates no indices, so the between-test index wipe, template listing, task wait,
    // and feature-reset that ESRestTestCase.cleanUpCluster() issues after each test are pure overhead.
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @AfterClass
    public static void cleanupFixtureDir() {
        try {
            Files.walkFileTree(FIXTURE_DIR, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.warn("Failed to delete fixture directory [{}]", FIXTURE_DIR, e);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Case definition
    // -----------------------------------------------------------------------------------------------------------------

    record ComboCase(String resourceFile, Map<String, Object> datasetSettings, boolean expectAccepted) {}

    /**
     * Returns true when a dimension value is known to be rejected at PUT by the current coordinator
     * validation. Cases whose settings include at least one such value have {@code expectAccepted=false}
     * and treat a 400 response as an expected outcome (soft-pass). All other cases have
     * {@code expectAccepted=true} and treat a 400 as a harness failure — a possible validator
     * regression.
     */
    private static boolean isKnownCanary(String dimensionName) {
        return switch (dimensionName) {
            case "ss_neg",    // schema_sample_size=-1: coordinator rejects negative values
                "em_garbage"  // error_mode=garbage_mode: coordinator rejects unknown error modes
                -> true;
            default -> false;
        };
    }

    /**
     * Full Cartesian product over all dimension values — valid and garbage alike.
     *
     * <p>CSV:    format_det × encoding × delimiter × header_row × schema_sample_size → 2×3×6×3×4 = 432
     * <p>TSV:    format_det × encoding × delimiter × header_row × schema_sample_size → 2×3×6×3×4 = 432
     * <p>NDJSON: format_det × segment_size × schema_sample_size                      → 2×2×4     =  16
     * <p>Parquet: format_det × error_mode                                            → 2×5        =  10
     * <p>ORC:    format_det × error_mode                                             → 2×5        =  10
     */
    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> parameters() {
        List<Object[]> cases = new ArrayList<>();

        // Each inner String[] is { case-name-fragment, setting-value } where null means "omit the setting".
        // For format_det the layout is { case-name-fragment, explicit-format-or-null, resource-file }.
        // format_det: how the format is determined — "ext" infers from the file extension,
        // "exp" sets it explicitly via format: <name> on an extensionless file.

        addDelimitedFormatCases(cases, "csv", "simple.csv", "csv_noext", "csv");
        addDelimitedFormatCases(cases, "tsv", "simple.tsv", "tsv_noext", "tsv");

        // --- NDJSON matrix ---
        for (String[] formatDet : new String[][] {
            { "ext", null, "simple.ndjson" }, // format inferred from .ndjson extension
            { "exp", "ndjson", "ndjson_noext" }, // explicit format: ndjson on an extensionless file
            // TODO(esql-planning#1550): uncomment once a mismatched explicit format is rejected at PUT
            // { "exp_bad", "parquet", "ndjson_noext" }, // unknown format — not yet rejected at PUT (esql-planning#1550)
        }) {
            String det = formatDet[0];
            String explicitFormat = formatDet[1];
            String resourceFile = formatDet[2];

            for (String[] segmentSize : new String[][] {
                { "seg_default", null }, // omit → reader default
                { "seg_1mb", "1mb" },
                // TODO(esql-planning#1550): uncomment once segment_size values are validated at PUT
                // { "seg_tiny", "1b" }, // below 64 KiB minimum — not yet rejected at PUT (esql-planning#1550)
                // { "seg_garbage", "foobar" }, // unparseable size — not yet rejected at PUT (esql-planning#1550)
            }) {
                for (String[] sampleSize : new String[][] {
                    { "ss_default", null }, // omit → reader default
                    { "ss_1", "1" }, // minimum boundary
                    { "ss_100", "100" },
                    { "ss_neg", "-1" }, // negative — rejected at PUT by coordinator validation (canary)
                }) {
                    String name = "ndjson_" + det + "_" + segmentSize[0] + "_" + sampleSize[0];
                    Map<String, Object> settings = new LinkedHashMap<>();
                    if (explicitFormat != null) settings.put("format", explicitFormat);
                    if (segmentSize[1] != null) settings.put("segment_size", segmentSize[1]);
                    if (sampleSize[1] != null) settings.put("schema_sample_size", Integer.parseInt(sampleSize[1]));
                    boolean expectAccepted = isKnownCanary(sampleSize[0]) == false;
                    cases.add(new Object[] { name, new ComboCase(resourceFile, Map.copyOf(settings), expectAccepted) });
                }
            }
        }

        // --- Parquet matrix ---
        // Parquet has no format-specific config keys.
        // schema_sample_size is in DATASET_FIELDS (coordinator base key) so the validator accepts it,
        // but the Parquet reader rejects it at query time — omitted here until the validator is tightened
        // (esql-planning#1743). error_mode is a true coordinator key that works end-to-end for all formats.
        addColumnarFormatCases(cases, "parquet", "simple.parquet", "parquet_noext", "parquet");

        // --- ORC matrix ---
        // Same rationale as Parquet: no format-specific keys; error_mode is the coordinator dimension.
        addColumnarFormatCases(cases, "orc", "simple.orc", "orc_noext", "orc");

        return cases;
    }

    /**
     * Builds the Cartesian product for columnar formats (Parquet and ORC share the same structure).
     * Dimensions: format_det × error_mode → 2×5 = 10 cases per format.
     */
    private static void addColumnarFormatCases(List<Object[]> cases, String prefix, String extFile, String noextFile, String formatName) {
        for (String[] formatDet : new String[][] {
            { "ext", null, extFile },           // format inferred from file extension
            { "exp", formatName, noextFile },   // explicit format on an extensionless file
        }) {
            String det = formatDet[0];
            String explicitFormat = formatDet[1];
            String resourceFile = formatDet[2];

            for (String[] errorMode : new String[][] {
                { "em_default", null },             // omit → fail_fast (default)
                { "em_fail_fast", "fail_fast" },    // explicit default
                { "em_skip_row", "skip_row" },      // lenient: skip malformed rows
                { "em_null_field", "null_field" },  // permissive: null-fill bad fields
                { "em_garbage", "garbage_mode" },   // invalid value — rejected at PUT by coordinator validation (canary)
            }) {
                String name = prefix + "_" + det + "_" + errorMode[0];
                Map<String, Object> settings = new LinkedHashMap<>();
                if (explicitFormat != null) settings.put("format", explicitFormat);
                if (errorMode[1] != null) settings.put("error_mode", errorMode[1]);
                boolean expectAccepted = isKnownCanary(errorMode[0]) == false;
                cases.add(new Object[] { name, new ComboCase(resourceFile, Map.copyOf(settings), expectAccepted) });
            }
        }
    }

    /**
     * Builds the Cartesian product for delimiter-separated formats (CSV and TSV share the same
     * FORMAT_CONFIG_KEYS set). Extracted to avoid duplicating the 5-level nested loop.
     */
    private static void addDelimitedFormatCases(List<Object[]> cases, String prefix, String extFile, String noextFile, String formatName) {
        for (String[] formatDet : new String[][] {
            { "ext", null, extFile },      // format inferred from file extension
            { "exp", formatName, noextFile }, // explicit format on an extensionless file
            // TODO(esql-planning#1550): uncomment once a mismatched explicit format is rejected at PUT
            // { "exp_bad", "parquet", noextFile }, // unknown format — not yet rejected at PUT (esql-planning#1550)
        }) {
            String det = formatDet[0];
            String explicitFormat = formatDet[1];
            String resourceFile = formatDet[2];

            for (String[] encoding : new String[][] {
                { "enc_default", null },        // omit → reader default (UTF-8)
                { "enc_utf8", "UTF-8" },
                { "enc_latin1", "ISO-8859-1" },
                // TODO(esql-planning#1550): uncomment once charset values are validated at PUT
                // { "enc_garbage", "UTF-99" }, // invalid charset — not yet rejected at PUT (esql-planning#1550)
            }) {
                for (String[] delimiter : new String[][] {
                    { "del_default", null },  // omit → format default (comma for CSV, tab for TSV)
                    { "del_pipe", "|" },
                    { "del_semi", ";" },
                    { "del_tab", "\t" },
                    { "del_empty", "" },      // empty string — accepted at PUT; reader treats as format default
                    { "del_multi", "||" },    // multi-char: silently truncated to '|' today (esql-planning#1550)
                }) {
                    for (String[] headerRow : new String[][] {
                        { "hdr_default", null },    // omit → true
                        { "hdr_true", "true" },     // explicit true — same behaviour, different code path
                        { "hdr_false", "false" }, }) {
                        for (String[] sampleSize : new String[][] {
                            { "ss_default", null }, // omit → reader default
                            { "ss_1", "1" },        // minimum boundary
                            { "ss_100", "100" },
                            { "ss_neg", "-1" },     // negative — rejected at PUT by coordinator validation (canary)
                        }) {
                            String name = prefix
                                + "_"
                                + det
                                + "_"
                                + encoding[0]
                                + "_"
                                + delimiter[0]
                                + "_"
                                + headerRow[0]
                                + "_"
                                + sampleSize[0];
                            Map<String, Object> settings = new LinkedHashMap<>();
                            if (explicitFormat != null) settings.put("format", explicitFormat);
                            if (encoding[1] != null) settings.put("encoding", encoding[1]);
                            if (delimiter[1] != null) settings.put("delimiter", delimiter[1]);
                            if (headerRow[1] != null) settings.put("header_row", Booleans.parseBoolean(headerRow[1]));
                            if (sampleSize[1] != null) settings.put("schema_sample_size", Integer.parseInt(sampleSize[1]));
                            boolean expectAccepted = isKnownCanary(sampleSize[0]) == false;
                            cases.add(new Object[] { name, new ComboCase(resourceFile, Map.copyOf(settings), expectAccepted) });
                        }
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Test body
    // -----------------------------------------------------------------------------------------------------------------

    private final String caseName;
    private final ComboCase combo;

    public DataSourceCrudAcceptCombinationsIT(String caseName, ComboCase combo) {
        this.caseName = caseName;
        this.combo = combo;
    }

    public void testCombination() throws IOException {
        String dtName = "combo_dt_" + caseName;
        String resourceUri = FIXTURE_DIR.resolve(combo.resourceFile()).toUri().toString();

        boolean datasetCreated = false;
        try {
            try {
                DatasetRegistry.putDataset(client(), dtName, SHARED_DS_NAME, resourceUri, combo.datasetSettings());
                datasetCreated = true;
            } catch (ResponseException e) {
                int status = e.getResponse().getStatusLine().getStatusCode();
                if (status != 400) {
                    throw e; // only validation rejections (400) are soft-passed; 404/409 are infra failures
                }
                if (combo.expectAccepted()) {
                    // 400 on a case expected to be accepted → possible validator regression
                    throw new AssertionError("PUT returned 400 for [" + caseName + "] which is expected to be accepted", e);
                }
                // Canary case: PUT correctly rejected the known-garbage value; skip the query
                return;
            }
            if (combo.expectAccepted() == false) {
                // Canary case: PUT was expected to return 400 but returned 200 — coordinator validation gap
                throw new AssertionError("PUT returned 200 for canary case [" + caseName + "] — expected 400 rejection");
            }
            runQuery("FROM " + dtName + " | LIMIT 1");
        } finally {
            if (datasetCreated) {
                try {
                    DatasetRegistry.deleteIgnoringMissing(client(), "/_query/dataset/" + dtName);
                } catch (Exception e) {
                    logger.warn("Failed to delete dataset [{}] during teardown", dtName, e);
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Fixture setup
    // -----------------------------------------------------------------------------------------------------------------

    private static Path initFixtureDir() {
        try {
            Path dir = Files.createTempDirectory(PathUtils.get(System.getProperty("java.io.tmpdir")), "esql-crud-combos-");
            // ASCII-only content — see class Javadoc for the encoding TODO.
            Files.writeString(dir.resolve("simple.csv"), "a,b\n1,foo\n2,bar\n");
            Files.writeString(dir.resolve("csv_noext"), "a,b\n1,foo\n2,bar\n");
            Files.writeString(dir.resolve("simple.tsv"), "a\tb\n1\tfoo\n2\tbar\n");
            Files.writeString(dir.resolve("tsv_noext"), "a\tb\n1\tfoo\n2\tbar\n");
            Files.writeString(dir.resolve("simple.ndjson"), "{\"a\":1,\"b\":\"foo\"}\n{\"a\":2,\"b\":\"bar\"}\n");
            Files.writeString(dir.resolve("ndjson_noext"), "{\"a\":1,\"b\":\"foo\"}\n{\"a\":2,\"b\":\"bar\"}\n");
            // Parquet: reuse employees.parquet already on the classpath from generateParquetFixtures.
            // ORC: generated at build time by generateCrudComboOrc (no equivalent pre-existing fixture).
            // Both use the employees schema; text-format fixtures use a,b — schemas differ but LIMIT 1 doesn't care.
            copyResource("/iceberg-fixtures/standalone/employees.parquet", dir.resolve("simple.parquet"));
            Files.copy(dir.resolve("simple.parquet"), dir.resolve("parquet_noext"));
            copyResource("/simple.orc", dir.resolve("simple.orc"));
            Files.copy(dir.resolve("simple.orc"), dir.resolve("orc_noext"));
            return dir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create combination fixture directory", e);
        }
    }

    private static void copyResource(String resourcePath, Path target) throws IOException {
        try (InputStream is = DataSourceCrudAcceptCombinationsIT.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Test resource not found on classpath: " + resourcePath);
            }
            Files.copy(is, target);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------------------------------------------------

    private void runQuery(String query) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", query).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response response = client().performRequest(req);
        // A 200 with empty values[] means a setting silently dropped all rows (e.g. error_mode mis-wired).
        // Assert at least one row so such regressions are not hidden behind HTTP 200.
        @SuppressWarnings("unchecked")
        List<?> values = (List<?>) entityAsMap(response).get("values");
        assertThat("response missing values field", values, notNullValue());
        assertThat("Query returned no rows — a dataset setting may have silently dropped all rows", values, not(empty()));
    }
}
