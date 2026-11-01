/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * The same files under the same resolved schema read the same rows, whether the schema was declared or inferred.
 * Every headered file binds its columns by its own header, never by position against another file's schema: a
 * reordered file reads each value under its own name, a wider file reads every row, and a narrower file reads the
 * column it lacks as null with one warning.
 */
public class ExternalProvenanceParityIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, Bzip2DataSourcePlugin.class);
    }

    private static Map<String, Object> firstFileWins(String format) {
        return Map.of("format", format, "schema_resolution", "first_file_wins", "file_sort_by", "name", "file_order", "asc");
    }

    private static LinkedHashMap<String, DatasetFieldMapping> idNameCity() {
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("name", new DatasetFieldMapping("keyword", null));
        properties.put("city", new DatasetFieldMapping("keyword", null));
        return properties;
    }

    private static final List<List<Object>> REORDERED_EXPECTED = List.of(
        List.of(1, "alice", "paris"),
        List.of(2, "carol", "berlin"),
        List.of(3, "bob", "tokyo"),
        List.of(4, "dave", "lima")
    );

    /** Runs {@code query} against every dataset and asserts each returns {@code expected}. */
    private void assertEveryDatasetReads(String query, List<List<Object>> expected, String... datasets) {
        for (String dataset : datasets) {
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + query), TIMEOUT)) {
                assertThat(dataset, getValuesList(response), equalTo(expected));
            }
        }
    }

    private record Answer(List<List<Object>> values, List<String> warnings) {}

    /**
     * Runs {@code query} and returns its values together with the response's warning headers. The headers are read from
     * the thread context of the node the query was sent to: another node's context on the listener's thread can still
     * carry the headers of an earlier query.
     */
    private Answer runCapturingWarnings(String query) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Answer> answer = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        String coordinator = randomFrom(clusterService().state().nodes().stream().toList()).getName();
        client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.wrap(response -> {
            try {
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator)
                    .getThreadPool()
                    .getThreadContext();
                List<String> warnings = new CopyOnWriteArrayList<>(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
                answer.set(new Answer(getValuesList(response), warnings));
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw failure.get();
        }
        return answer.get();
    }

    private static List<String> containing(List<String> warnings, String substring) {
        return warnings.stream().filter(w -> w.contains(substring)).toList();
    }

    public void testReorderedHeaderReadsTheSameDeclaredOrInferred() throws Exception {
        Path dir = createTempDir().resolve("reordered");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name,city\n1,alice,paris\n2,carol,berlin\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,city,name\n3,tokyo,bob\n4,lima,dave\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        Map<String, Object> settings = firstFileWins("csv");

        String inferred = registerLocalFileDataset("reordered_inferred", glob, settings);
        String declared = registerStrictDataset("reordered_declared", glob, idNameCity(), settings);

        assertEveryDatasetReads(" | SORT id | KEEP id, name, city", REORDERED_EXPECTED, declared, inferred);
    }

    public void testReorderedTsvHeaderReadsTheSameDeclaredOrInferred() throws Exception {
        Path dir = createTempDir().resolve("reordered_tsv");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.tsv"), "id\tname\tcity\n1\talice\tparis\n2\tcarol\tberlin\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.tsv"), "id\tcity\tname\n3\ttokyo\tbob\n4\tlima\tdave\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.tsv";
        Map<String, Object> settings = firstFileWins("tsv");

        String inferred = registerLocalFileDataset("reordered_tsv_inferred", glob, settings);
        String declared = registerStrictDataset("reordered_tsv_declared", glob, idNameCity(), settings);

        assertEveryDatasetReads(" | SORT id | KEEP id, name, city", REORDERED_EXPECTED, declared, inferred);
    }

    /** A mapping under {@code dynamic: true} is inferred and overlaid; its files bind by their own headers too. */
    public void testReorderedHeaderReadsTheSameUnderANonStrictMapping() throws Exception {
        Path dir = createTempDir().resolve("reordered_dynamic");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name,city\n1,alice,paris\n2,carol,berlin\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,city,name\n3,tokyo,bob\n4,lima,dave\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        Map<String, Object> settings = firstFileWins("csv");

        String inferred = registerLocalFileDataset("reordered_dynamic_inferred", glob, settings);
        String nonStrict = registerNonStrictDataset("reordered_dynamic_declared", glob, idNameCity(), settings);

        assertEveryDatasetReads(" | SORT id | KEEP id, name, city", REORDERED_EXPECTED, nonStrict, inferred);
    }

    /**
     * A file one column narrower than the resolved schema reads that column as null, with one warning, rather than
     * failing the query before any row is read.
     */
    public void testFileMissingAResolvedColumnReadsNullWithOneWarning() throws Exception {
        Path dir = createTempDir().resolve("narrower");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name,extra\n1,alice,x1\n2,carol,x2\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,name\n3,bob\n4,dave\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        Map<String, Object> settings = firstFileWins("csv");

        String inferred = registerLocalFileDataset("narrower_inferred", glob, settings);
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("name", new DatasetFieldMapping("keyword", null));
        properties.put("extra", new DatasetFieldMapping("keyword", null));
        String declared = registerStrictDataset("narrower_declared", glob, properties, settings);

        List<List<Object>> expected = List.of(
            List.of(1, "alice", "x1"),
            List.of(2, "carol", "x2"),
            Arrays.asList(3, "bob", null),
            Arrays.asList(4, "dave", null)
        );
        List<Answer> answers = new ArrayList<>();
        for (String dataset : List.of(declared, inferred)) {
            Answer answer = runCapturingWarnings("FROM " + dataset + " | SORT id | KEEP id, name, extra");
            assertThat(dataset, answer.values(), equalTo(expected));
            assertThat(
                dataset + " warns once that b.csv lacks the column, got " + answer.warnings(),
                containing(answer.warnings(), "column [extra] is not present in some source files and reads null there"),
                hasSize(1)
            );
            answers.add(answer);
        }
        assertThat("declared and inferred read the same rows", answers.get(0).values(), equalTo(answers.get(1).values()));
    }

    /**
     * A file one column wider than the resolved schema reads every row and leaves its extra column unread, rather than
     * failing every row of that file against the first file's width.
     */
    public void testFileWithAnExtraColumnReadsEveryRow() throws Exception {
        Path dir = createTempDir().resolve("wider");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name\n1,alice\n2,carol\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,name,extra\n3,bob,x\n4,dave,y\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        Map<String, Object> settings = firstFileWins("csv");

        String inferred = registerLocalFileDataset("wider_inferred", glob, settings);
        String declared = registerStrictDataset("wider_declared", glob, idName(), settings);

        List<List<Object>> expected = List.of(List.of(1, "alice"), List.of(2, "carol"), List.of(3, "bob"), List.of(4, "dave"));
        assertEveryDatasetReads(" | SORT id | KEEP id, name", expected, declared, inferred);
    }

    /**
     * Under {@code skip_row} a wider file's rows are not errors to drop: every row is counted and nothing warns about a
     * dropped row.
     */
    public void testFileWithAnExtraColumnDropsNoRowUnderSkipRow() throws Exception {
        Path dir = createTempDir().resolve("wider_skip_row");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name\n1,alice\n2,carol\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,name,extra\n3,bob,x\n4,dave,y\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        Map<String, Object> settings = new LinkedHashMap<>(firstFileWins("csv"));
        settings.put("error_mode", "skip_row");

        String inferred = registerLocalFileDataset("wider_skip_row_inferred", glob, settings);
        String declared = registerStrictDataset("wider_skip_row_declared", glob, idName(), settings);

        for (String dataset : List.of(declared, inferred)) {
            Answer answer = runCapturingWarnings("FROM " + dataset + " | STATS c = COUNT(*)");
            assertThat(dataset, answer.values(), equalTo(List.of(List.of(4L))));
            assertThat(dataset + " must drop no row, got " + answer.warnings(), containing(answer.warnings(), "columns but"), empty());
            assertThat(dataset + " must skip no row, got " + answer.warnings(), containing(answer.warnings(), "skip"), empty());
        }
    }

    /**
     * A later file whose header names a column twice cannot bind by name, so the query fails and names the file, under
     * every {@code error_mode}: it is a malformed file, not a malformed row. The same for a declared and an inferred
     * schema.
     */
    public void testLaterFileWithADuplicateHeaderNameFailsTheQuery() throws Exception {
        Path dir = createTempDir().resolve("duplicate_header");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name\n1,alice\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,name,id\n2,bob,3\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        for (String errorMode : List.of("fail_fast", "skip_row")) {
            Map<String, Object> settings = new LinkedHashMap<>(firstFileWins("csv"));
            settings.put("error_mode", errorMode);
            String inferred = registerLocalFileDataset("duplicate_header_inferred_" + errorMode, glob, settings);
            String declared = registerStrictDataset("duplicate_header_declared_" + errorMode, glob, idName(), settings);
            for (String dataset : List.of(declared, inferred)) {
                Exception e = expectThrows(
                    Exception.class,
                    () -> run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, name"), TIMEOUT).close()
                );
                assertThat(dataset, e.getMessage(), containsString("b.csv] has duplicate column name [id]; columns cannot bind by name"));
            }
        }
    }

    /**
     * Headerless files are outside the by-header binding and keep their own rules, which this pins. A declared schema
     * binds {@code col<N>} to field N and states no row width, so a later, wider file reads every row and the extra
     * fields are ignored. An inferred schema binds by position and bounds rows by its own width, taken from the first
     * file, so the same wider file is a row error.
     */
    public void testHeaderlessWiderLaterFileKeepsItsBindingByProvenance() throws Exception {
        Path dir = createTempDir().resolve("headerless_multifile");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "1,alice\n2,carol\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "3,bob,tokyo\n4,dave,lima,extra\n", StandardCharsets.UTF_8);
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        Map<String, Object> settings = new LinkedHashMap<>(firstFileWins("csv"));
        settings.put("header_row", false);
        LinkedHashMap<String, DatasetFieldMapping> declaredColumns = new LinkedHashMap<>();
        declaredColumns.put("id", new DatasetFieldMapping("integer", "col0"));
        declaredColumns.put("name", new DatasetFieldMapping("keyword", "col1"));

        String declared = registerStrictDataset("headerless_multifile_declared", glob, declaredColumns, settings);
        String inferred = registerLocalFileDataset("headerless_multifile_inferred", glob, settings);

        assertEveryDatasetReads(
            " | SORT id | KEEP id, name",
            List.of(List.of(1, "alice"), List.of(2, "carol"), List.of(3, "bob"), List.of(4, "dave")),
            declared
        );
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM " + inferred + " | SORT col0 | KEEP col0, col1"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("columns, the schema has [2]"));
    }

    private static LinkedHashMap<String, DatasetFieldMapping> idName() {
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("name", new DatasetFieldMapping("keyword", null));
        return properties;
    }

    /**
     * A declared headered CSV larger than one split is split like an inferred one, and every split past the first binds
     * the declared columns by the file's header. The declaration orders the columns differently from the file, so a
     * split bound by position would read the wrong column.
     */
    public void testDeclaredHeaderedCsvLargerThanOneSplitReadsEveryRow() throws Exception {
        Path dir = createTempDir();
        int rows = 200_000;
        Path file = dir.resolve("large.csv");
        Files.writeString(file, largeCsv(rows, 0, new Random(randomLong())), StandardCharsets.UTF_8);
        assertThat("the file must exceed several splits", Files.size(file), greaterThan(3L * 1024 * 1024));
        assertEverySplitBindsByHeader("large", StoragePath.fileUri(file), Map.of("format", "csv", "target_split_size", "1mb"), rows);
    }

    /**
     * The schema is taken from a small first file, and a second file larger than one split lists its columns in another
     * order. Every split of the second file, not only the first, must bind by that file's own header: a first split bound
     * by name and later splits bound by position would count only the rows of the first split.
     */
    public void testInferredSchemaFromASmallFileBindsEverySplitOfALargeReorderedFileByHeader() throws Exception {
        Path dir = createTempDir().resolve("anchored");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name,salary\n-1,name-1,-2\n", StandardCharsets.UTF_8);
        int rows = 200_000;
        StringBuilder reordered = new StringBuilder("salary,id,name\n");
        for (int i = 0; i < rows; i++) {
            reordered.append(2L * i).append(',').append(i).append(",name").append(i).append('\n');
        }
        Path large = dir.resolve("b.csv");
        Files.writeString(large, reordered, StandardCharsets.UTF_8);
        assertThat("the second file must exceed several splits", Files.size(large), greaterThan(3L * 1024 * 1024));

        Map<String, Object> settings = new LinkedHashMap<>(firstFileWins("csv"));
        settings.put("target_split_size", "1mb");
        String inferred = registerLocalFileDataset("anchored_inferred", StoragePath.fileUri(dir) + "/*.csv", settings);

        var request = syncEsqlQueryRequest(
            "FROM " + inferred + " | WHERE name == CONCAT(\"name\", TO_STRING(id)) AND salary == 2 * id | STATS c = COUNT(*)"
        );
        request.profile(true);
        try (var response = run(request, TimeValue.timeValueMinutes(5))) {
            assertThat(
                "every row of both files must bind by its own header",
                ((Number) getValuesList(response).get(0).get(0)).longValue(),
                equalTo((long) rows + 1)
            );
            assertThat(
                "the large file must be read as more than one split",
                response.getExecutionInfo().queryProfile().splitsScanned(),
                greaterThan(2)
            );
        }

        // An ungrouped COUNT(*) projects no column, which takes its own route to the file's header on a later split.
        try (var response = run(syncEsqlQueryRequest("FROM " + inferred + " | STATS c = COUNT(*)"), TimeValue.timeValueMinutes(5))) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo((long) rows + 1));
        }
    }

    /**
     * A file larger than one split whose records are all comments has no header and no data. It reads no rows, and does
     * not fail the later splits for want of a header they cannot be given.
     */
    public void testMultiSplitFileOfOnlyCommentsReadsNoRows() throws Exception {
        Path dir = createTempDir().resolve("comments");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,name\n1,alice\n2,bob\n", StandardCharsets.UTF_8);
        StringBuilder comments = new StringBuilder();
        for (int i = 0; i < 150_000; i++) {
            comments.append("# comment line number ").append(i).append('\n');
        }
        Path large = dir.resolve("b.csv");
        Files.writeString(large, comments, StandardCharsets.UTF_8);
        assertThat("the comment-only file must exceed several splits", Files.size(large), greaterThan(3L * 1024 * 1024));

        Map<String, Object> settings = new LinkedHashMap<>(firstFileWins("csv"));
        settings.put("target_split_size", "1mb");
        settings.put("comment", "#");
        String inferred = registerLocalFileDataset("comments_inferred", StoragePath.fileUri(dir) + "/*.csv", settings);

        try (var response = run(syncEsqlQueryRequest("FROM " + inferred + " | SORT id | KEEP id, name"), TimeValue.timeValueMinutes(5))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "alice"), List.of(2, "bob"))));
        }
    }

    /**
     * The same for a bzip2 file, which is cut at compressed block boundaries into splits of at least 32 MiB of compressed
     * bytes, whatever {@code target_split_size} says. A split past the first starts inside a record and reads its header
     * columns from the file's decompressed first line.
     */
    public void testDeclaredHeaderedBzip2CsvLargerThanOneSplitReadsEveryRow() throws Exception {
        assumeTrue("bzip2 is available on snapshot builds only", Build.current().isSnapshot());
        Path dir = createTempDir();
        // Each row carries a random 96-character base64 pad so the file resists compression: the split target is
        // measured in compressed bytes.
        int rows = 500_000;
        Path file = dir.resolve("large.csv.bz2");
        writeBzip2(file, largeCsv(rows, 96, new Random(randomLong())));
        assertThat("the compressed file must exceed one 32 MiB split", Files.size(file), greaterThan(34L * 1024 * 1024));
        // mode=plain: a quoted compressed file is never cut past its first byte, whatever its header.
        assertEverySplitBindsByHeader(
            "large_bz2",
            StoragePath.fileUri(file),
            Map.of("format", "csv", "mode", "plain", "target_split_size", "1mb"),
            rows
        );
    }

    /**
     * A headerless bzip2 file is cut the same way, and a split past the first starts inside a record the previous split
     * already read. Under a pinned schema that partial record must be dropped, as it is for a headered file: read as a row
     * it fails the query, or under a lenient policy adds a row that is not in the file.
     */
    public void testHeaderlessBzip2CsvLargerThanOneSplitReadsEveryRowOnce() throws Exception {
        assumeTrue("bzip2 is available on snapshot builds only", Build.current().isSnapshot());
        Path dir = createTempDir();
        int rows = 500_000;
        String withHeader = largeCsv(rows, 96, new Random(randomLong()));
        Path file = dir.resolve("large.csv.bz2");
        writeBzip2(file, withHeader.substring(withHeader.indexOf('\n') + 1));
        assertThat("the compressed file must exceed one 32 MiB split", Files.size(file), greaterThan(34L * 1024 * 1024));
        Map<String, Object> settings = Map.of("format", "csv", "mode", "plain", "header_row", false, "target_split_size", "1mb");

        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("salary", new DatasetFieldMapping("long", "col2"));
        properties.put("name", new DatasetFieldMapping("keyword", "col1"));
        properties.put("id", new DatasetFieldMapping("long", "col0"));
        String declared = registerStrictDataset("headerless_bz2_declared", StoragePath.fileUri(file), properties, settings);

        var request = syncEsqlQueryRequest(
            "FROM " + declared + " | WHERE name == CONCAT(\"name\", TO_STRING(id)) AND salary == 2 * id | STATS c = COUNT(*)"
        );
        request.profile(true);
        try (var response = run(request, TimeValue.timeValueMinutes(5))) {
            assertThat(
                "every row read exactly once, none from a partial record",
                ((Number) getValuesList(response).get(0).get(0)).longValue(),
                equalTo((long) rows)
            );
            assertThat(
                "the file must be read as more than one split",
                response.getExecutionInfo().queryProfile().splitsScanned(),
                greaterThan(1)
            );
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + declared + " | STATS c = COUNT(*)"), TimeValue.timeValueMinutes(5))) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo((long) rows));
        }
    }

    /** {@code id,name,salary[,pad]} rows where {@code name == "name" + id} and {@code salary == 2 * id}. */
    private static String largeCsv(int rows, int padChars, Random random) {
        StringBuilder csv = new StringBuilder(rows * (32 + padChars));
        csv.append(padChars > 0 ? "id,name,salary,pad\n" : "id,name,salary\n");
        byte[] padBytes = new byte[padChars * 3 / 4];
        Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
        for (int i = 0; i < rows; i++) {
            csv.append(i).append(",name").append(i).append(',').append(2L * i);
            if (padChars > 0) {
                random.nextBytes(padBytes);
                csv.append(',').append(encoder.encodeToString(padBytes));
            }
            csv.append('\n');
        }
        return csv.toString();
    }

    /**
     * Registers {@code resource} inferred and declared (declared in the order salary, name, id — not the file's), reads
     * every row with a filter that only a by-name binding satisfies, and asserts both see every row across more than one
     * split.
     */
    private void assertEverySplitBindsByHeader(String prefix, String resource, Map<String, Object> settings, int rows) {
        String inferred = registerLocalFileDataset(prefix + "_inferred", resource, settings);
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("salary", new DatasetFieldMapping("long", null));
        properties.put("name", new DatasetFieldMapping("keyword", null));
        properties.put("id", new DatasetFieldMapping("long", null));
        String declared = registerStrictDataset(prefix + "_declared", resource, properties, settings);

        List<List<Object>> results = new ArrayList<>();
        for (String dataset : List.of(declared, inferred)) {
            var request = syncEsqlQueryRequest(
                "FROM " + dataset + " | WHERE name == CONCAT(\"name\", TO_STRING(id)) AND salary == 2 * id | STATS c = COUNT(*)"
            );
            request.profile(true);
            try (var response = run(request, TimeValue.timeValueMinutes(5))) {
                List<List<Object>> values = getValuesList(response);
                assertThat(
                    dataset + " must read every row bound by name",
                    ((Number) values.get(0).get(0)).longValue(),
                    equalTo((long) rows)
                );
                assertThat(
                    dataset + " must be read as more than one split",
                    response.getExecutionInfo().queryProfile().splitsScanned(),
                    greaterThan(1)
                );
                results.add(values.get(0));
            }
        }
        assertThat("declared and inferred read the same rows", results.get(0), equalTo(results.get(1)));
    }
}
