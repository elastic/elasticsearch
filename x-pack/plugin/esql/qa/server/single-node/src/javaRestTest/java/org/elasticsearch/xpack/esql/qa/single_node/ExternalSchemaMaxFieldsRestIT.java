/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end check that {@code esql.external.schema_max_fields} (default 1000) and a dataset's
 * {@code schema_max_fields} stop schema inference over a file with too many columns, for every format that infers one.
 * Resolution runs on the coordinating node during planning, so an over-wide file used to be able to take the node down
 * before the query ran; here it must come back as an error response and leave the node serving.
 *
 * <p>The text formats use a file one column over the default cap. Parquet fixtures need a writer this module does not
 * have, so it reuses {@code employees.parquet} (more than one column) with a dataset cap of {@code 1}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ExternalSchemaMaxFieldsRestIT extends ESRestTestCase {

    /** One more than the node default of {@code esql.external.schema_max_fields}. */
    private static final int WIDE_COLUMNS = 1001;

    private static final Logger logger = LogManager.getLogger(ExternalSchemaMaxFieldsRestIT.class);

    private static final String DATA_SOURCE = "schema_cap_ds";

    private static final Path FIXTURE_DIR = initFixtureDir();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(FIXTURE_DIR, config -> {}, false);

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @Before
    public void ensureDataSource() throws IOException {
        DatasetRegistry.ensureDataSource(client(), DATA_SOURCE, "local", Map.of());
    }

    private final List<String> datasets = new ArrayList<>();

    @After
    public void deleteDatasets() throws IOException {
        for (String dataset : datasets) {
            DatasetRegistry.deleteIgnoringMissing(client(), "/_query/dataset/" + dataset);
        }
        datasets.clear();
    }

    private void putDataset(String name, String file, Map<String, Object> settings) throws IOException {
        DatasetRegistry.putDataset(client(), name, DATA_SOURCE, uri(file), settings);
        datasets.add(name);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (Build.current().isSnapshot()) {
            DatasetRegistry.cleanup(client());
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /** A file one column over the default cap is refused, and the node stays up to serve the next request. */
    public void testWideFileOverDefaultCapIsRefused() throws IOException {
        for (String file : List.of("wide.csv", "wide.tsv", "wide.ndjson")) {
            String dataset = datasetName("wide_default", file);
            putDataset(dataset, file, Map.of());
            assertRefused(dataset);
            assertNodeStillServes();
        }
    }

    /** A dataset's {@code schema_max_fields} raises the cap for that dataset. */
    public void testDatasetCapRaisesTheLimit() throws IOException {
        for (String file : List.of("wide.csv", "wide.tsv", "wide.ndjson")) {
            String dataset = datasetName("wide_raised", file);
            putDataset(dataset, file, Map.of("schema_max_fields", 2 * WIDE_COLUMNS));
            Response response = query(dataset);
            assertThat(file, values(response), not(empty()));
        }
    }

    /** A dataset's {@code schema_max_fields} also lowers the cap, which is how Parquet is exercised without a wide fixture. */
    public void testDatasetCapLowersTheLimit() throws IOException {
        for (String file : List.of("narrow.csv", "narrow.tsv", "narrow.ndjson", "employees.parquet")) {
            String dataset = datasetName("narrow_lowered", file);
            putDataset(dataset, file, Map.of("schema_max_fields", 1));
            assertRefused(dataset);
        }
    }

    /** Parquet with a cap that admits its schema reads normally. */
    public void testParquetWithinDatasetCapIsRead() throws IOException {
        String dataset = datasetName("parquet_within", "employees.parquet");
        putDataset(dataset, "employees.parquet", Map.of("schema_max_fields", 1000));
        assertThat(values(query(dataset)), not(empty()));
    }

    /**
     * Multi-file datasets under each {@code schema_resolution}. Files are ordered by name, so {@code a_narrow} is the
     * first file (the anchor for {@code first_file_wins}) and {@code z_wide} is a later one; the {@code wide_first}
     * directory puts the wide file first.
     */
    public void testMultiFileWideAnchorIsRefusedUnderEveryResolution() throws IOException {
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            for (String resolution : List.of("first_file_wins", "union_by_name", "strict")) {
                String dataset = datasetName("multi_anchor_" + resolution, ext);
                putGlobDataset(dataset, "wide_first_" + ext, ext, resolution, Map.of());
                assertRefused(dataset);
            }
        }
    }

    public void testMultiFileWideLaterFileIsRefusedWhenEveryFileIsInferred() throws IOException {
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            for (String resolution : List.of("union_by_name", "strict")) {
                String dataset = datasetName("multi_later_" + resolution, ext);
                putGlobDataset(dataset, "wide_last_" + ext, ext, resolution, Map.of());
                assertRefused(dataset);
            }
        }
    }

    /**
     * {@code first_file_wins} infers only the anchor, so a wide later file is only met when a query reads it. The text
     * formats refuse it then (a 429) and NDJSON refuses it when the query needs its statistics, but an NDJSON
     * {@code KEEP} that skips the statistics path reads the wide file with no cap. That last case is a known gap, kept
     * under its own test so the gap is visible and the test fails when it is closed.
     */
    public void testMultiFileFirstFileWinsWithWideLaterFile() throws IOException {
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            String dataset = datasetName("multi_later_ffw", ext);
            putGlobDataset(dataset, "wide_last_" + ext, ext, "first_file_wins", Map.of());
            // LIMIT 1 is answered by the narrow anchor alone, so the wide file is never opened.
            assertThat(ext, values(esql("FROM " + dataset + " | LIMIT 1")), hasSize(1));
            assertQueryRefused("FROM " + dataset + " | STATS c = COUNT(*)");
            if (ext.equals("ndjson") == false) {
                assertQueryRefused("FROM " + dataset + " | KEEP a, b | LIMIT 100");
            }
            assertNodeStillServes();
        }
    }

    /**
     * Known gap: under {@code first_file_wins} an NDJSON {@code KEEP} reads a wide later file without checking its
     * width, so the query returns the narrow file's two rows plus the wide file's one. Making it uniform with the text
     * formats needs a read-time width probe. When that lands, this test should assert a 429 instead.
     */
    public void testNdjsonFirstFileWinsKeepReadsWideLaterFileKnownGap() throws IOException {
        String dataset = datasetName("multi_later_ffw_keep", "ndjson");
        putGlobDataset(dataset, "wide_last_ndjson", "ndjson", "first_file_wins", Map.of());
        assertThat(values(esql("FROM " + dataset + " | KEEP a, b | LIMIT 100")), hasSize(3));
    }

    /**
     * Each file has two columns, under a cap of three, but their names differ so the merged {@code union_by_name} schema
     * has four. The per-file cap does not see that, so the merge is refused; a cap of four admits it.
     */
    public void testUnionByNameMergedSchemaOverCapIsRefused() throws IOException {
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            String refused = datasetName("union_over_cap", ext);
            putGlobDataset(refused, "union_diverge_" + ext, ext, "union_by_name", Map.of("schema_max_fields", 3));
            assertRefused(refused);

            String admitted = datasetName("union_at_cap", ext);
            putGlobDataset(admitted, "union_diverge_" + ext, ext, "union_by_name", Map.of("schema_max_fields", 4));
            assertThat(ext, values(query(admitted)), not(empty()));
        }
    }

    /** Parquet over a glob: the cap applies to the anchor and, for the merging modes, to every file. */
    public void testMultiFileParquetUnderEveryResolution() throws IOException {
        for (String resolution : List.of("first_file_wins", "union_by_name", "strict")) {
            String dataset = datasetName("multi_parquet", resolution);
            putGlobDataset(dataset, "parquet_multi", "parquet", resolution, Map.of("schema_max_fields", 1));
            assertRefused(dataset);
        }
    }

    private void putGlobDataset(String dataset, String dir, String ext, String resolution, Map<String, Object> extra) throws IOException {
        Map<String, Object> settings = new LinkedHashMap<>(extra);
        settings.put("schema_resolution", resolution);
        if (resolution.equals("first_file_wins")) {
            settings.put("file_sort_by", "name");
        }
        DatasetRegistry.putDataset(client(), dataset, DATA_SOURCE, FIXTURE_DIR.resolve(dir).toUri() + "*." + ext, settings);
        datasets.add(dataset);
    }

    /**
     * A declared schema names the columns it reads, so a file one column over the cap is read normally: the cap bounds
     * what inference materialises. Single files and a glob whose files differ in width are both read.
     */
    public void testDeclaredSchemaOverWideFileIsRead() throws IOException {
        Map<String, Object> mappings = Map.of(
            "dynamic",
            "false",
            "properties",
            Map.of("c5", Map.of("type", "long"), "c1000", Map.of("type", "long"))
        );
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            String dataset = datasetName("declared_wide", ext);
            DatasetRegistry.putDataset(client(), dataset, DATA_SOURCE, uri("wide." + ext), Map.of(), mappings);
            datasets.add(dataset);
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\":\"FROM " + dataset + " | KEEP c5, c1000 | LIMIT 10\"}");
            assertThat(ext, values(client().performRequest(request)), equalTo(List.of(List.of(1, 1))));

            String glob = datasetName("declared_wide_glob", ext);
            DatasetRegistry.putDataset(
                client(),
                glob,
                DATA_SOURCE,
                FIXTURE_DIR.resolve("wide_last_" + ext).toUri() + "*." + ext,
                Map.of(),
                mappings
            );
            datasets.add(glob);
            Request globRequest = new Request("POST", "/_query");
            globRequest.setJsonEntity("{\"query\":\"FROM " + glob + " | STATS c = COUNT(*)\"}");
            globRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            // The narrow file has no c5 or c1000, so those read null there; the file count is what matters.
            assertThat(ext, values(client().performRequest(globRequest)), equalTo(List.of(List.of(3))));
        }
    }

    /**
     * A declared {@code mappings} block is held to the cap like an inferred schema: two declared columns against a cap
     * of one is refused, and a cap of two admits it.
     */
    public void testDeclaredMappingOverCapIsRefused() throws IOException {
        Map<String, Object> mappings = Map.of(
            "dynamic",
            "false",
            "properties",
            Map.of("a", Map.of("type", "long"), "b", Map.of("type", "keyword"))
        );
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            String refused = datasetName("declared_over_cap", ext);
            DatasetRegistry.putDataset(client(), refused, DATA_SOURCE, uri("narrow." + ext), Map.of("schema_max_fields", 1), mappings);
            datasets.add(refused);
            assertRefused(refused);

            String admitted = datasetName("declared_at_cap", ext);
            DatasetRegistry.putDataset(client(), admitted, DATA_SOURCE, uri("narrow." + ext), Map.of("schema_max_fields", 2), mappings);
            datasets.add(admitted);
            assertThat(ext, values(query(admitted)), not(empty()));
        }
    }

    /**
     * Parquet has no declared exemption: it resolves the footer's schema on every path, so a declared dataset over a
     * file wider than the cap is refused, naming the setting to raise.
     */
    public void testDeclaredParquetIsHeldToTheCap() throws IOException {
        Map<String, Object> mappings = Map.of("dynamic", "false", "properties", Map.of("emp_no", Map.of("type", "integer")));
        String dataset = datasetName("declared_parquet_cap", "employees.parquet");
        DatasetRegistry.putDataset(client(), dataset, DATA_SOURCE, uri("employees.parquet"), Map.of("schema_max_fields", 1), mappings);
        datasets.add(dataset);
        assertRefused(dataset);
    }

    private void assertRefused(String dataset) throws IOException {
        assertQueryRefused("FROM " + dataset + " | LIMIT 1");
    }

    private void assertQueryRefused(String query) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> esql(query));
        int status = e.getResponse().getStatusLine().getStatusCode();
        String body = EntityUtils.toString(e.getResponse().getEntity());
        logger.info("[{}] refused with status [{}]: {}", query, status, body);
        // A circuit-breaking refusal (429), never a dropped connection, a 400 blaming the data, or a 500: the node must
        // answer. The message names the setting to change.
        assertThat(body, status, equalTo(429));
        assertThat(body, containsString("circuit_breaking_exception"));
        assertThat(body, containsString("schema_max_fields"));
    }

    private void assertNodeStillServes() throws IOException {
        assertThat(client().performRequest(new Request("GET", "/")).getStatusLine().getStatusCode(), equalTo(200));
    }

    private static List<?> values(Response response) throws IOException {
        return (List<?>) entityAsMap(response).get("values");
    }

    private Response query(String dataset) throws IOException {
        return esql("FROM " + dataset + " | LIMIT 1");
    }

    /** Runs {@code query}, tolerating the warnings a partly-null declared or multi-file read emits. */
    private Response esql(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\":\"" + query + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return client().performRequest(request);
    }

    private static String uri(String file) {
        return FIXTURE_DIR.resolve(file).toUri().toString();
    }

    private static String datasetName(String prefix, String file) {
        return prefix + "_" + file.replaceAll("[^a-z0-9]", "_");
    }

    private static Path initFixtureDir() {
        try {
            Path dir = Files.createTempDirectory(PathUtils.get(System.getProperty("java.io.tmpdir")), "esql-schema-cap-");
            List<String> names = IntStream.range(0, WIDE_COLUMNS).mapToObj(i -> "c" + i).toList();
            String ones = IntStream.range(0, WIDE_COLUMNS).mapToObj(i -> "1").collect(Collectors.joining(","));
            Files.writeString(dir.resolve("wide.csv"), String.join(",", names) + "\n" + ones + "\n");
            Files.writeString(dir.resolve("wide.tsv"), String.join("\t", names) + "\n" + ones.replace(',', '\t') + "\n");
            Files.writeString(
                dir.resolve("wide.ndjson"),
                names.stream().map(n -> "\"" + n + "\":1").collect(Collectors.joining(",", "{", "}")) + "\n"
            );
            Files.writeString(dir.resolve("narrow.csv"), "a,b\n1,foo\n2,bar\n");
            Files.writeString(dir.resolve("narrow.tsv"), "a\tb\n1\tfoo\n2\tbar\n");
            Files.writeString(dir.resolve("narrow.ndjson"), "{\"a\":1,\"b\":\"foo\"}\n{\"a\":2,\"b\":\"bar\"}\n");
            try (
                InputStream is = ExternalSchemaMaxFieldsRestIT.class.getResourceAsStream("/iceberg-fixtures/standalone/employees.parquet")
            ) {
                if (is == null) {
                    throw new IOException("Test resource not found on classpath: employees.parquet");
                }
                Files.copy(is, dir.resolve("employees.parquet"));
            }
            for (String ext : List.of("csv", "tsv", "ndjson")) {
                Path wideFirst = Files.createDirectory(dir.resolve("wide_first_" + ext));
                Path wideLast = Files.createDirectory(dir.resolve("wide_last_" + ext));
                Files.copy(dir.resolve("wide." + ext), wideFirst.resolve("a_wide." + ext));
                Files.copy(dir.resolve("narrow." + ext), wideFirst.resolve("z_narrow." + ext));
                Files.copy(dir.resolve("narrow." + ext), wideLast.resolve("a_narrow." + ext));
                Files.copy(dir.resolve("wide." + ext), wideLast.resolve("z_wide." + ext));
            }
            Files.createDirectory(dir.resolve("union_diverge_csv"));
            Files.writeString(dir.resolve("union_diverge_csv/a.csv"), "a,b\n1,foo\n");
            Files.writeString(dir.resolve("union_diverge_csv/b.csv"), "c,d\n2,bar\n");
            Files.createDirectory(dir.resolve("union_diverge_tsv"));
            Files.writeString(dir.resolve("union_diverge_tsv/a.tsv"), "a\tb\n1\tfoo\n");
            Files.writeString(dir.resolve("union_diverge_tsv/b.tsv"), "c\td\n2\tbar\n");
            Files.createDirectory(dir.resolve("union_diverge_ndjson"));
            Files.writeString(dir.resolve("union_diverge_ndjson/a.ndjson"), "{\"a\":1,\"b\":\"foo\"}\n");
            Files.writeString(dir.resolve("union_diverge_ndjson/b.ndjson"), "{\"c\":2,\"d\":\"bar\"}\n");
            Path parquetMulti = Files.createDirectory(dir.resolve("parquet_multi"));
            Files.copy(dir.resolve("employees.parquet"), parquetMulti.resolve("a.parquet"));
            Files.copy(dir.resolve("employees.parquet"), parquetMulti.resolve("b.parquet"));
            return dir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create schema cap fixture directory", e);
        }
    }
}
