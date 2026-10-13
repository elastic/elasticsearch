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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

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
 * <p>Every format has a file one column over the default cap. The text formats are written here; {@code wide.parquet} is
 * {@code wide_schema.parquet}, which the build converts from {@code wide_schema.csv}. Parquet globs reuse
 * {@code employees.parquet} (more than one column) with a dataset cap of {@code 1}.
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
        for (String file : List.of("wide.csv", "wide.tsv", "wide.ndjson", "wide.parquet")) {
            String dataset = datasetName("wide_default", file);
            putDataset(dataset, file, Map.of());
            assertRefused(dataset);
            assertNodeStillServes();
        }
    }

    /** A dataset's {@code schema_max_fields} raises the cap for that dataset. */
    public void testDatasetCapRaisesTheLimit() throws IOException {
        for (String file : List.of("wide.csv", "wide.tsv", "wide.ndjson", "wide.parquet")) {
            String dataset = datasetName("wide_raised", file);
            putDataset(dataset, file, Map.of("schema_max_fields", 2 * WIDE_COLUMNS));
            Response response = query(dataset);
            assertThat(file, values(response), not(empty()));
        }
    }

    /** A dataset's {@code schema_max_fields} also lowers the cap. */
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
     * {@code first_file_wins} resolves a schema from the anchor alone, so the cap is met there and nowhere else. A wide
     * later file is read: the schema is already settled, and that file's header is split only to locate the anchor's two
     * columns in it, which is bounded separately. CSV and TSV used to refuse that file unless the dataset declared its
     * columns, so the same glob was readable or not by how it was written down (esql-planning#2076).
     * <p>
     * {@code COUNT(*)} runs first, before anything has warmed this glob's files, and with every column pruned - the one
     * shape where the reader is pinned to nothing and so cannot tell a settled schema from none. A {@code KEEP} ahead of
     * it would warm the file and answer from the cache instead.
     */
    public void testMultiFileFirstFileWinsWithWideLaterFile() throws IOException {
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            String dataset = datasetName("multi_later_ffw", ext);
            putGlobDataset(dataset, "wide_last_" + ext, ext, "first_file_wins", Map.of());
            // Two rows in the narrow anchor and one in the wide file.
            assertThat(ext, values(esql("FROM " + dataset + " | STATS c = COUNT(*)")), equalTo(List.of(List.of(3))));
            // LIMIT 1 is answered by the narrow anchor alone, so the wide file is never opened.
            assertThat(ext, values(esql("FROM " + dataset + " | LIMIT 1")), hasSize(1));
            // The pinned two-column schema reads the wide file's row as nulls.
            assertThat(ext, values(esql("FROM " + dataset + " | KEEP a, b | LIMIT 100")), hasSize(3));
            assertNodeStillServes();
        }
    }

    /**
     * The same glob gzipped. A compressed file is read by the streaming parse coordinator, which resolves the first
     * chunk's schema after the planner has settled one, so holding that resolve to the cap refused a file the
     * uncompressed read admitted: one file, two outcomes, decided by how it was compressed (esql-planning#2076).
     */
    public void testMultiFileFirstFileWinsWithWideLaterGzippedFile() throws IOException {
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            String dataset = datasetName("multi_later_ffw_gz", ext);
            putGlobDataset(dataset, "wide_last_gz_" + ext, ext + ".gz", "first_file_wins", Map.of());
            assertThat(ext, values(esql("FROM " + dataset + " | STATS c = COUNT(*)")), equalTo(List.of(List.of(3))));
            assertThat(ext, values(esql("FROM " + dataset + " | KEEP a, b | LIMIT 100")), hasSize(3));
            assertNodeStillServes();
        }
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
     * A file one column over the cap is read normally when the dataset names the columns it reads. The cap bounds the
     * schema a read resolves, and this read resolves none: the header is split to locate {@code c5} and {@code c1000},
     * which is bounded separately, so the file's own width does not decide whether a two-column schema can be read.
     * Single files and a glob whose files differ in width are both read.
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
     * The same declared read of a gzipped file. A compressed file is read by the streaming parse coordinator, whose
     * resolve of the first chunk runs after the planner has settled the declared schema, so the file's own width must
     * not decide the read there either (esql-planning#2076). {@code COUNT(*)} runs first, with every column pruned and
     * nothing warmed: the state in which the reader carries no schema of its own, so only the schema the planner
     * settled can bound that resolve. The first-file-wins twin above is the case that was refused before the bound
     * moved; this one guards the declared side of the same decision against a later change to how the pin travels.
     */
    public void testDeclaredSchemaOverWideGzippedFileIsRead() throws IOException {
        Map<String, Object> mappings = Map.of(
            "dynamic",
            "false",
            "properties",
            Map.of("c5", Map.of("type", "long"), "c1000", Map.of("type", "long"))
        );
        for (String ext : List.of("csv", "tsv", "ndjson")) {
            String dataset = datasetName("declared_wide_gz", ext);
            DatasetRegistry.putDataset(client(), dataset, DATA_SOURCE, uri("wide." + ext + ".gz"), Map.of(), mappings);
            datasets.add(dataset);
            assertThat(ext, values(esql("FROM " + dataset + " | STATS c = COUNT(*)")), equalTo(List.of(List.of(1))));
            assertThat(ext, values(esql("FROM " + dataset + " | KEEP c5, c1000 | LIMIT 10")), equalTo(List.of(List.of(1, 1))));
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
     * A Parquet dataset that names its columns is not held to the width of its files, the same as the text formats, and
     * for the same reason: the read resolves no schema from the footer, so there is no schema for the cap to bound. One
     * named column over a file wider than a cap of one reads, both for a single file and for a glob whose files are all
     * wider than the cap, so the outcome does not depend on which file the planner reads. A file one column over the
     * default cap reads the same way.
     */
    public void testDeclaredParquetIsNotHeldToTheFileWidth() throws IOException {
        Map<String, Object> mappings = Map.of("dynamic", "false", "properties", Map.of("emp_no", Map.of("type", "integer")));
        String dataset = datasetName("declared_parquet_cap", "employees.parquet");
        DatasetRegistry.putDataset(client(), dataset, DATA_SOURCE, uri("employees.parquet"), Map.of("schema_max_fields", 1), mappings);
        datasets.add(dataset);
        assertThat(values(query(dataset)), not(empty()));

        String glob = datasetName("declared_parquet_glob_cap", "parquet_multi");
        DatasetRegistry.putDataset(
            client(),
            glob,
            DATA_SOURCE,
            FIXTURE_DIR.resolve("parquet_multi").toUri() + "*.parquet",
            Map.of("schema_max_fields", 1),
            mappings
        );
        datasets.add(glob);
        assertThat(values(query(glob)), not(empty()));

        // A file one column over the default cap, read through two declared columns.
        Map<String, Object> wideMappings = Map.of(
            "dynamic",
            "false",
            "properties",
            Map.of("c5", Map.of("type", "long"), "c1000", Map.of("type", "long"))
        );
        String wide = datasetName("declared_parquet_wide", "wide.parquet");
        DatasetRegistry.putDataset(client(), wide, DATA_SOURCE, uri("wide.parquet"), Map.of(), wideMappings);
        datasets.add(wide);
        assertThat(values(esql("FROM " + wide + " | KEEP c5, c1000 | LIMIT 10")), equalTo(List.of(List.of(1, 1))));
    }

    private void assertRefused(String dataset) throws IOException {
        assertQueryRefused("FROM " + dataset + " | LIMIT 1");
    }

    private void assertQueryRefused(String query) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> esql(query));
        int status = e.getResponse().getStatusLine().getStatusCode();
        String body = EntityUtils.toString(e.getResponse().getEntity());
        logger.info("[{}] refused with status [{}]: {}", query, status, body);
        // A configured limit, so a 400 the client should not retry: never a 429, a dropped connection or a 500. The node
        // must answer, and the message names the setting to change.
        assertThat(body, status, equalTo(400));
        assertThat(body, containsString("Schema has too many columns"));
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
            copyParquetFixture("employees.parquet", dir.resolve("employees.parquet"));
            copyParquetFixture("wide_schema.parquet", dir.resolve("wide.parquet"));
            for (String ext : List.of("csv", "tsv", "ndjson")) {
                Path wideFirst = Files.createDirectory(dir.resolve("wide_first_" + ext));
                Path wideLast = Files.createDirectory(dir.resolve("wide_last_" + ext));
                Files.copy(dir.resolve("wide." + ext), wideFirst.resolve("a_wide." + ext));
                Files.copy(dir.resolve("narrow." + ext), wideFirst.resolve("z_narrow." + ext));
                Files.copy(dir.resolve("narrow." + ext), wideLast.resolve("a_narrow." + ext));
                Files.copy(dir.resolve("wide." + ext), wideLast.resolve("z_wide." + ext));
            }
            for (String ext : List.of("csv", "tsv", "ndjson")) {
                writeGzipped(dir.resolve("wide." + ext + ".gz"), dir.resolve("wide." + ext));
                Path wideLastGz = Files.createDirectory(dir.resolve("wide_last_gz_" + ext));
                writeGzipped(wideLastGz.resolve("a_narrow." + ext + ".gz"), dir.resolve("narrow." + ext));
                writeGzipped(wideLastGz.resolve("z_wide." + ext + ".gz"), dir.resolve("wide." + ext));
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

    /** Gzips {@code source} into {@code target}, so a dataset over it is read by the streaming parse coordinator. */
    private static void writeGzipped(Path target, Path source) throws IOException {
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(target))) {
            Files.copy(source, out);
        }
    }

    /** Copies a Parquet file the build generated from a CSV fixture. */
    private static void copyParquetFixture(String name, Path target) throws IOException {
        try (InputStream is = ExternalSchemaMaxFieldsRestIT.class.getResourceAsStream("/iceberg-fixtures/standalone/" + name)) {
            if (is == null) {
                throw new IOException("Test resource not found on classpath: " + name);
            }
            Files.copy(is, target);
        }
    }
}
