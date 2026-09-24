/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.example.data.Group;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests that verify name-based column binding for CSV (and TSV) globs under
 * {@code first_file_wins} schema resolution.
 *
 * <h2>The problem</h2>
 * When a glob covers multiple CSV files and the schema is pinned to the first (anchor) file,
 * every non-anchor file was previously read with positional binding. If its header names the
 * same columns in a different order — or has a different column count — the result was silent
 * data corruption or an exception:
 * <ul>
 *   <li><b>Reordered columns</b>: values end up under the wrong names.</li>
 *   <li><b>Wider second file</b>: "CSV row has [N] columns but schema defines [M] columns".</li>
 *   <li><b>Narrower second file</b>: "pinned schema has N columns but file has only M".</li>
 * </ul>
 *
 * <h2>The fix</h2>
 * {@code declaredProvenanceBinding} is now also enabled for anchor-pinned {@code first_file_wins}
 * reads, so every file in the glob matches its columns by name rather than by position.
 *
 * <h2>Regression-guard tests</h2>
 * Parquet and NDJSON globs, and CSV globs under {@code union_by_name} or a declared mapping, already
 * bound by name before this fix. Those tests confirm the fix does not regress those paths.
 */
public class ExternalCsvMultiFileGlobBindingIT extends AbstractExternalDataSourceIT {

    /** Ensures the lexicographically first file ("a.*") anchors the schema. */
    private static final Map<String, Object> ANCHOR_FIRST = Map.of("file_sort_by", "name", "file_order", "asc");

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    // -----------------------------------------------------------------------
    // Fix-verification tests — these all FAILED before the fix
    // -----------------------------------------------------------------------

    /**
     * A second CSV file whose header lists the same columns in a different order must bind by
     * name rather than by position; values must appear under their correct column names.
     */
    public void testReorderedHeaderBindsByName() throws Exception {
        Path dir = createTempDir("glob-reorder-csv");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword,city:keyword\n1,alice,paris\n2,carol,berlin\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,city:keyword,name:keyword\n3,tokyo,bob\n4,lima,dave\n");
        String dataset = registerLocalFileDataset("reorder_csv", StoragePath.fileUri(dir) + "/*.csv", ANCHOR_FIRST);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        List.of(3, "bob", "tokyo"),
                        List.of(4, "dave", "lima")
                    )
                )
            );
        }
    }

    /**
     * A second CSV file that has MORE columns than the anchor schema must not throw; the extra
     * column is simply ignored and its rows still contribute their anchor-schema values.
     */
    public void testWiderGlobFileStillContributesRows() throws Exception {
        Path dir = createTempDir("glob-wider-csv");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword\n1,alice\n2,carol\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,name:keyword,city:keyword\n3,bob,tokyo\n4,dave,lima\n");
        String dataset = registerLocalFileDataset("wider_csv", StoragePath.fileUri(dir) + "/*.csv", ANCHOR_FIRST);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "alice"), List.of(2, "carol"), List.of(3, "bob"), List.of(4, "dave")))
            );
        }
    }

    /**
     * A second CSV file that has FEWER columns than the anchor schema must not throw; the missing
     * column reads null for every row that comes from that file.
     */
    public void testNarrowerGlobFileStillContributesRows() throws Exception {
        Path dir = createTempDir("glob-narrower-csv");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword,city:keyword\n1,alice,paris\n2,carol,berlin\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,name:keyword\n3,bob\n4,dave\n");
        String dataset = registerLocalFileDataset("narrower_csv", StoragePath.fileUri(dir) + "/*.csv", ANCHOR_FIRST);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        Arrays.asList(3, "bob", null),
                        Arrays.asList(4, "dave", null)
                    )
                )
            );
        }
    }

    /**
     * TSV files go through the same {@code CsvFormatReader} with a tab delimiter;
     * the same name-binding fix must apply.
     */
    public void testReorderedTsvHeaderBindsByName() throws Exception {
        Path dir = createTempDir("glob-reorder-tsv");
        Files.writeString(dir.resolve("a.tsv"), "id:integer\tname:keyword\tcity:keyword\n1\talice\tparis\n2\tcarol\tberlin\n");
        Files.writeString(dir.resolve("b.tsv"), "id:integer\tcity:keyword\tname:keyword\n3\ttokyo\tbob\n4\tlima\tdave\n");
        String dataset = registerLocalFileDataset("reorder_tsv", StoragePath.fileUri(dir) + "/*.tsv", ANCHOR_FIRST);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        List.of(3, "bob", "tokyo"),
                        List.of(4, "dave", "lima")
                    )
                )
            );
        }
    }

    /**
     * When the parallel parser splits a file across multiple chunks, non-first chunks must also
     * receive the captured file-header columns so they bind by name, not by position.
     * Forces parallelism via {@code external_parsing_parallelism} and a moderately large file.
     */
    public void testReorderedHeaderBindsByNameOnEverySplit() throws Exception {
        Path dir = createTempDir("glob-multisplit-csv");
        StringBuilder anchor = new StringBuilder("id:integer,name:keyword,city:keyword\n");
        StringBuilder second = new StringBuilder("id:integer,city:keyword,name:keyword\n");
        for (int i = 1; i <= 500; i++) {
            anchor.append(i).append(",name").append(i).append(",city").append(i).append("\n");
        }
        for (int i = 501; i <= 1000; i++) {
            second.append(i).append(",city").append(i).append(",name").append(i).append("\n");
        }
        Files.writeString(dir.resolve("a.csv"), anchor.toString());
        Files.writeString(dir.resolve("b.csv"), second.toString());
        String dataset = registerLocalFileDataset("multisplit_csv", StoragePath.fileUri(dir) + "/*.csv", ANCHOR_FIRST);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1000L))));
        }
        // Spot-check a row from b.csv: 'name' must come from the 'name' column, not from the 'city' position.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | WHERE id == 501 | KEEP id, name, city"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(501, "name501", "city501"))));
        }
    }

    /**
     * Wider-file scenario with lenient error handling ({@code error_handling=skip}): extra columns
     * in the second file must not cause rows to be skipped, because name binding makes those columns
     * invisible to the row-width validator.
     */
    public void testWiderGlobFileStillContributesRowsLenient() throws Exception {
        Path dir = createTempDir("glob-wider-lenient-csv");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword\n1,alice\n2,carol\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,name:keyword,city:keyword\n3,bob,tokyo\n4,dave,lima\n");
        Map<String, Object> settings = Map.of("file_sort_by", "name", "file_order", "asc", "error_mode", "skip_row");
        String dataset = registerLocalFileDataset("wider_lenient_csv", StoragePath.fileUri(dir) + "/*.csv", settings);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "alice"), List.of(2, "carol"), List.of(3, "bob"), List.of(4, "dave")))
            );
        }
    }

    // -----------------------------------------------------------------------
    // Regression-guard tests — these all PASSED before the fix
    // -----------------------------------------------------------------------

    /**
     * Parquet already binds columns by name from its per-file schema metadata; a reordered glob
     * must not regress after the fix.
     */
    public void testParquetReorderedColumnsBindByName() throws Exception {
        Path dir = createTempDir("glob-reorder-parquet");
        writeParquet(
            dir.resolve("a.parquet"),
            "message r { required int32 id; required binary name (UTF8); required binary city (UTF8); }",
            2,
            1024,
            (Group g, int i) -> {
                g.add("id", i + 1);
                g.add("name", i == 0 ? "alice" : "carol");
                g.add("city", i == 0 ? "paris" : "berlin");
            }
        );
        writeParquet(
            dir.resolve("b.parquet"),
            "message r { required int32 id; required binary city (UTF8); required binary name (UTF8); }",
            2,
            1024,
            (Group g, int i) -> {
                g.add("id", i + 3);
                g.add("city", i == 0 ? "tokyo" : "lima");
                g.add("name", i == 0 ? "bob" : "dave");
            }
        );
        String dataset = registerLocalFileDataset("reorder_parquet", StoragePath.fileUri(dir) + "/*.parquet", ANCHOR_FIRST);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        List.of(3, "bob", "tokyo"),
                        List.of(4, "dave", "lima")
                    )
                )
            );
        }
    }

    /**
     * NDJSON already binds values by JSON key; a reordered-key glob must not regress after the fix.
     */
    public void testReorderedNdJsonKeysBindByName() throws Exception {
        Path dir = createTempDir("glob-reorder-ndjson");
        Files.writeString(dir.resolve("a.ndjson"), """
            {"id":1,"name":"alice","city":"paris"}
            {"id":2,"name":"carol","city":"berlin"}
            """);
        Files.writeString(dir.resolve("b.ndjson"), """
            {"city":"tokyo","id":3,"name":"bob"}
            {"city":"lima","id":4,"name":"dave"}
            """);
        String dataset = registerLocalFileDataset("reorder_ndjson", StoragePath.fileUri(dir) + "/*.ndjson", ANCHOR_FIRST);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        List.of(3, "bob", "tokyo"),
                        List.of(4, "dave", "lima")
                    )
                )
            );
        }
    }

    /**
     * {@code union_by_name} resolution already built a per-file declared schema that forced name
     * binding; a reordered CSV glob under that resolution must not regress after the fix.
     */
    public void testReorderedHeaderUnderUnionByName() throws Exception {
        Path dir = createTempDir("glob-reorder-ubn");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword,city:keyword\n1,alice,paris\n2,carol,berlin\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,city:keyword,name:keyword\n3,tokyo,bob\n4,lima,dave\n");
        String dataset = registerLocalFileDataset(
            "reorder_ubn",
            StoragePath.fileUri(dir) + "/*.csv",
            Map.of("schema_resolution", "union_by_name")
        );
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        List.of(3, "bob", "tokyo"),
                        List.of(4, "dave", "lima")
                    )
                )
            );
        }
    }

    /**
     * {@code union_by_name} with a wider second file must still return all rows projected to the
     * anchor columns, without throwing or dropping rows.
     */
    public void testWiderGlobFileUnderUnionByName() throws Exception {
        Path dir = createTempDir("glob-wider-ubn");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword\n1,alice\n2,carol\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,name:keyword,city:keyword\n3,bob,tokyo\n4,dave,lima\n");
        String dataset = registerLocalFileDataset(
            "wider_ubn",
            StoragePath.fileUri(dir) + "/*.csv",
            Map.of("schema_resolution", "union_by_name")
        );
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "alice"), List.of(2, "carol"), List.of(3, "bob"), List.of(4, "dave")))
            );
        }
    }

    /**
     * {@code union_by_name} with a narrower second file must null-fill the missing column for
     * every row from that file, without throwing.
     */
    public void testNarrowerGlobFileUnderUnionByName() throws Exception {
        Path dir = createTempDir("glob-narrower-ubn");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword,city:keyword\n1,alice,paris\n2,carol,berlin\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,name:keyword\n3,bob\n4,dave\n");
        String dataset = registerLocalFileDataset(
            "narrower_ubn",
            StoragePath.fileUri(dir) + "/*.csv",
            Map.of("schema_resolution", "union_by_name")
        );
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        Arrays.asList(3, "bob", null),
                        Arrays.asList(4, "dave", null)
                    )
                )
            );
        }
    }

    /**
     * A declared (strict) mapping already forced name binding; a reordered CSV glob under such a
     * mapping must not regress after the fix.
     */
    public void testReorderedHeaderUnderDeclaredMapping() throws Exception {
        Path dir = createTempDir("glob-reorder-declared");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword,city:keyword\n1,alice,paris\n2,carol,berlin\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,city:keyword,name:keyword\n3,tokyo,bob\n4,lima,dave\n");
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("id", new DatasetFieldMapping("integer", null));
        props.put("name", new DatasetFieldMapping("keyword", null));
        props.put("city", new DatasetFieldMapping("keyword", null));
        String dataset = registerStrictDataset("reorder_declared", StoragePath.fileUri(dir) + "/*.csv", props, Map.of());
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        List.of(3, "bob", "tokyo"),
                        List.of(4, "dave", "lima")
                    )
                )
            );
        }
    }

    /**
     * Per-file preamble skipping ({@code skip_rows: 2}) must still work after the name-binding fix:
     * two comment lines before the header in each file must be skipped, and a reordered second file
     * must still bind its columns by name after those preamble rows are consumed.
     */
    public void testSkipRowsAppliesToDatasetPreambleOnly() throws Exception {
        Path dir = createTempDir("glob-skip-rows");
        Files.writeString(
            dir.resolve("a.csv"),
            "## preamble\n## preamble\nid:integer,name:keyword,city:keyword\n1,alice,paris\n2,carol,berlin\n"
        );
        Files.writeString(
            dir.resolve("b.csv"),
            "## preamble\n## preamble\nid:integer,city:keyword,name:keyword\n3,tokyo,bob\n4,lima,dave\n"
        );
        Map<String, Object> settings = Map.of("file_sort_by", "name", "file_order", "asc", "skip_rows", 2);
        String dataset = registerLocalFileDataset("skip_rows_glob", StoragePath.fileUri(dir) + "/*.csv", settings);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response).get(0).get(0), equalTo(4L));
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name, city | SORT id"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "alice", "paris"),
                        List.of(2, "carol", "berlin"),
                        List.of(3, "bob", "tokyo"),
                        List.of(4, "dave", "lima")
                    )
                )
            );
        }
    }

    /**
     * The correctly reordered-header result must not be marked partial and must not emit any warning
     * about a file, a column, or a schema. The pre-fix answer was also un-warned (the bug was
     * silent); this test pins that property for the correct answer.
     */
    public void testReorderedHeaderReportsNoWarningAndIsNotPartial() throws Exception {
        Path dir = createTempDir("glob-reorder-warn");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword,city:keyword\n1,alice,paris\n2,carol,berlin\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,city:keyword,name:keyword\n3,tokyo,bob\n4,lima,dave\n");
        String dataset = registerLocalFileDataset("reorder_nowarn", StoragePath.fileUri(dir) + "/*.csv", ANCHOR_FIRST);
        String query = "FROM " + dataset + " | SORT id | KEEP id, name, city";

        try (var response = run(syncEsqlQueryRequest(query))) {
            assertFalse("result must not be marked partial", response.isPartial());
        }
        List<String> fileColumnOrSchemaWarnings = collectWarnings(query).stream()
            .filter(w -> w.contains("file") || w.contains("column") || w.contains("schema"))
            .toList();
        assertThat("reordered-header query must emit no file/column/schema warnings", fileColumnOrSchemaWarnings, empty());
    }

    /**
     * A glob file with a duplicate column name in its header cannot be read; the query must fail
     * with a message identifying the duplicate. The duplicate is detected during schema scanning
     * (before the query executes), so the error is "CSV header has duplicate column names".
     */
    public void testDuplicateHeaderColumnNameFails() throws Exception {
        Path dir = createTempDir("glob-dup-header");
        Files.writeString(dir.resolve("a.csv"), "id:integer,name:keyword,city:keyword\n1,alice,paris\n");
        Files.writeString(dir.resolve("b.csv"), "id:integer,name:keyword,name:keyword\n2,bob,bogus\n");
        String dataset = registerLocalFileDataset("dup_header", StoragePath.fileUri(dir) + "/*.csv", ANCHOR_FIRST);
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("CSV header has duplicate column names"));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Runs the given ES|QL query and collects all response {@code Warning} headers. Uses the
     * transport-client listener pattern so that the thread context headers are readable inside the
     * callback before the response is released.
     */
    private List<String> collectWarnings(String query) throws Exception {
        List<String> warnings = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), new ActionListener<>() {
            @Override
            public void onResponse(EsqlQueryResponse r) {
                try {
                    internalCluster().getInstance(TransportService.class)
                        .getThreadPool()
                        .getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .forEach(warnings::add);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                latch.countDown();
            }
        });
        assertTrue("query did not complete within timeout", latch.await(30, SECONDS));
        if (failure.get() != null) {
            throw failure.get();
        }
        return warnings;
    }
}
