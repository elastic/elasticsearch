/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheTestAccess;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Multi-FILE warm short-circuit regression test. The sibling fold ITs
 * ({@link ExternalCsvMultiStripeFoldIT} / {@link ExternalNdJsonMultiStripeFoldIT}) only ever read ONE file:
 * they exercise the within-file per-stripe fold but never the cross-FILE merge of per-file whole-file
 * column statistics that the dataset-wide warm aggregate short-circuit depends on. That gap let a
 * regression through where, over a multi-file glob, warm {@code COUNT(*)} short-circuited correctly but
 * warm {@code MIN}/{@code MAX} full-scanned because the dataset-wide column min/max was never assembled
 * from the N per-file column stats.
 * <p>
 * Here we write {@code FILE_COUNT} separate files into a directory, glob them, and assert that after a cold
 * scan both {@code COUNT(*)} AND {@code MIN(col)}/{@code MAX(col)} short-circuit on the warm pass
 * ({@code documentsFound == 0}). COUNT(*) is run first (cold + warm) so its row-count-only per-file cache
 * entries are in place before MIN/MAX, reproducing the production ordering where COUNT short-circuits and
 * MIN/MAX must still serve from the merged dataset-wide column min/max. Run for CSV and NDJSON.
 * <p>
 * <b>Every harvest here is whole-file</b>, and the reason is that no query pragma reaches the request.
 * {@code AbstractEsqlIntegTestCase} attaches {@code getPragmas()} only from its {@code run(String, TimeValue)}
 * overload; every query below builds its own {@code syncEsqlQueryRequest} and goes through the
 * {@code run(EsqlQueryRequest, TimeValue)} override, so nothing selects the parallel-parse path. The
 * contributions this suite produces therefore classify as {@code WholeFile} and never as
 * {@code StripeFragment}, {@code applyStripeDelta} matches nothing, and the cross-file merge is fed by the
 * whole-file path alone. The per-stripe rail is covered by {@code ExternalMultiChunkPerStripeWarmFoldIT};
 * a divergent file read in chunk mode is covered by neither and is a gap, not a claim this suite makes.
 * <p>
 * The precise pre-fix-fail / post-fix-pass regression for the cross-file column-stat merge defect lives in
 * {@code MergedSplitStatsTests#testColumnMinMaxUsesChildValueWhenNullCountUnknownButMinMaxPresent}: a
 * per-file {@code SplitStats} carrying a folded min/max with an unknown null_count must still contribute its
 * extremum to the dataset-wide {@code MIN}/{@code MAX} instead of poisoning it. This end-to-end IT is the
 * multi-FILE coverage the single-file fold ITs lacked.
 */
public class ExternalMultiFileWarmAggregateFoldIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 25;
    private static final int ROWS_PER_FILE = 60_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // A tiny stripe grid, so the geometry here matches production rather than the default. It does
            // not make this suite exercise the per-stripe rail - no pragma selects the parallel parse, see the
            // class comment. ExternalMultiChunkPerStripeWarmFoldIT covers the per-stripe fold.
            .put("esql.external.cache.stripe.size", "64kb")
            .build();
    }

    /**
     * Pins every query to one coordinator: the reconciled schema cache is per-coordinator, so the cold
     * scan and the warm short-circuit must hit the same node (mirrors {@link ExternalNdJsonAggregatePushdownIT}).
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testCsvMultiFileWarmCountAndMinMaxShortCircuit() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeCsvFile(dir.resolve("part-" + f + ".csv"), total);
        }
        String dataset = registerDataset("multifile_csv", globUri(dir, "*.csv"), Map.of());
        assertWarmAggregatesShortCircuit(dataset, total);
    }

    public void testNdjsonMultiFileWarmCountAndMinMaxShortCircuit() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        String dataset = registerDataset("multifile_ndjson", globUri(dir, "*.ndjson"), Map.of());
        assertWarmAggregatesShortCircuit(dataset, total);
    }

    /**
     * {@code value} runs 0..total-1 globally across all files, so the dataset-wide MIN is 0 and MAX is
     * {@code total-1}. After a cold scan that reads every row, the warm COUNT(*) and the warm MIN/MAX must
     * both short-circuit to {@code LocalSourceExec} (0 documents scanned), proving the cross-file column
     * min/max merge served the answer.
     */
    private void assertWarmAggregatesShortCircuit(String dataset, long total) {
        // COUNT(*): cold scans, warm must short-circuit (this already worked; it is the control).
        String countQuery = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("cold COUNT(*) reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("warm COUNT(*) must short-circuit across many files", response.documentsFound(), equalTo(0L));
        }

        // MIN/MAX: cold scans, warm must short-circuit using the merged dataset-wide column min/max.
        String minMaxQuery = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("cold MIN/MAX reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat(
                "warm MIN/MAX must short-circuit across many files (dataset-wide column min/max merged from per-file stats)",
                response.documentsFound(),
                equalTo(0L)
            );
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Heterogeneous corpus: the parts do not all infer the same schema, in the two ways a real corpus produces.
    // ------------------------------------------------------------------------------------------------------------

    private static final int HET_ROWS_PER_FILE = 30_000;
    /** The part whose {@code color} column holds one letter inside the inference window, so it infers keyword. */
    private static final int MIXED_PART = 5;
    /** The parts whose {@code order_id} column is blank in every row, so inference has no evidence and falls to keyword. */
    private static final int SPARSE_FIRST_PART = 1;
    private static final int SPARSE_LAST_PART = 9;

    /**
     * Writes {@link #FILE_COUNT} parts of {@code id,color,order_id,value}. With {@code heterogeneous} the corpus
     * has the shape a real one takes: {@code color} is digits everywhere except one letter in part
     * {@link #MIXED_PART} inside the 20,000-row inference window (that part infers keyword, the rest integer), and
     * {@code order_id} is blank in every row of parts {@link #SPARSE_FIRST_PART}..{@link #SPARSE_LAST_PART}
     * (those parts fall to the keyword default, the rest infer integer). {@code value} runs 0..total-1 across the
     * parts and infers the same type in every part. Without {@code heterogeneous} every part infers the same schema.
     * Part names are zero-padded so {@code file_sort_by: name} makes part 00 the first-file-wins anchor.
     */
    private static long writeCsvCorpus(Path dir, boolean heterogeneous) throws IOException {
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            boolean sparse = heterogeneous && f >= SPARSE_FIRST_PART && f <= SPARSE_LAST_PART;
            StringBuilder sb = new StringBuilder("id,color,order_id,value\n");
            for (int i = 0; i < HET_ROWS_PER_FILE; i++) {
                long v = total + i;
                String color = heterogeneous && f == MIXED_PART && i == 10 ? "g" : Long.toString(v % 7);
                String orderId = sparse ? "" : Long.toString(v % 1000);
                sb.append(v).append(',').append(color).append(',').append(orderId).append(',').append(v).append('\n');
            }
            Files.writeString(dir.resolve(String.format(Locale.ROOT, "part-%02d.csv", f)), sb.toString(), StandardCharsets.UTF_8);
            total += HET_ROWS_PER_FILE;
        }
        return total;
    }

    /** {@code file_sort_by} is only accepted under first_file_wins, where it pins part 00 as the anchor. */
    private static Map<String, Object> nullFieldSettings(String schemaResolution) {
        return "first_file_wins".equals(schemaResolution)
            ? Map.of("format", "csv", "error_mode", "null_field", "schema_resolution", schemaResolution, "file_sort_by", "name")
            : Map.of("format", "csv", "error_mode", "null_field", "schema_resolution", schemaResolution);
    }

    private static LinkedHashMap<String, DatasetFieldMapping> declaredColumns() {
        LinkedHashMap<String, DatasetFieldMapping> columns = new LinkedHashMap<>();
        columns.put("id", new DatasetFieldMapping("integer", null));
        columns.put("color", new DatasetFieldMapping("keyword", null));
        columns.put("order_id", new DatasetFieldMapping("integer", null));
        columns.put("value", new DatasetFieldMapping("integer", null));
        return columns;
    }

    /** No row is dropped under {@code null_field} on this corpus, so the warm COUNT(*) must be served. */
    public void testCsvHeterogeneousCorpusWarmCountServedUnderNullFieldFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = writeCsvCorpus(dir, true);
        String dataset = registerDataset("het_ffw_csv", globUri(dir, "*.csv"), nullFieldSettings("first_file_wins"));
        assertWarmCountShortCircuits(dataset, total);
    }

    @AwaitsFix(bugUrl = "union_by_name retypes per file, and the pinned-column poison is not lifted yet")
    public void testCsvHeterogeneousCorpusWarmCountServedUnderNullFieldUnionByName() throws Exception {
        Path dir = createTempDir();
        long total = writeCsvCorpus(dir, true);
        String dataset = registerDataset("het_ubn_csv", globUri(dir, "*.csv"), nullFieldSettings("union_by_name"));
        assertWarmCountShortCircuits(dataset, total);
    }

    /** {@code strict} rejects a corpus whose parts disagree, so its arm runs the homogeneous corpus under {@code null_field}. */
    public void testCsvHomogeneousCorpusWarmCountServedUnderNullFieldStrict() throws Exception {
        Path dir = createTempDir();
        long total = writeCsvCorpus(dir, false);
        String dataset = registerDataset("hom_strict_csv", globUri(dir, "*.csv"), nullFieldSettings("strict"));
        assertWarmCountShortCircuits(dataset, total);
    }

    @AwaitsFix(bugUrl = "the non-strict declared overlay is not wired to the read-addressed statistics record")
    public void testCsvHeterogeneousCorpusWarmCountServedUnderNullFieldDeclaredDynamic() throws Exception {
        Path dir = createTempDir();
        long total = writeCsvCorpus(dir, true);
        String dataset = registerNonStrictDataset(
            "het_dyn_csv",
            globUri(dir, "*.csv"),
            declaredColumns(),
            nullFieldSettings("first_file_wins")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    /**
     * Two datasets over the same files with the same settings, one declaring its schema and one inferring it. They
     * bind their columns differently — by name against each file's own header, or by position — so they bound a
     * row's width differently and need not count the same rows. Neither may be served the other's memoized count.
     * <p>Fails if the dataset key stops carrying the binding mode: the strict dataset is then handed the inferred
     * one's count and answers its first query without reading anything.
     */
    @AwaitsFix(bugUrl = "the dataset aggregate carries no read configuration, so nothing separates a strict fold from an inferred one")
    public void testStrictAndInferredDatasetsOverOneGlobNeverShareAnAggregate() throws Exception {
        Path dir = createTempDir();
        long total = writeCsvCorpus(dir, true);
        String uri = globUri(dir, "*.csv");
        Map<String, Object> settings = Map.of("format", "csv", "error_mode", "null_field");

        String inferred = registerDataset("shared_glob_inferred_csv", uri, settings);
        assertWarmCountShortCircuits(inferred, total);

        // The inferred dataset's count is now memoized. A strict declaration over the same files must not receive it.
        String strict = registerStrictDataset("shared_glob_strict_csv", uri, declaredColumns(), settings);
        String countQuery = "FROM " + strict + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("a strict dataset must not be served the count another binding measured", response.documentsFound(), equalTo(total));
        }
    }

    @AwaitsFix(bugUrl = "the declared strict rail is not wired to the read-addressed statistics record")
    public void testCsvHeterogeneousCorpusWarmCountServedUnderNullFieldDeclaredStrict() throws Exception {
        Path dir = createTempDir();
        long total = writeCsvCorpus(dir, true);
        String dataset = registerStrictDataset(
            "het_declared_csv",
            globUri(dir, "*.csv"),
            declaredColumns(),
            Map.of("format", "csv", "error_mode", "null_field")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    /**
     * A ragged corpus read two ways that SHARE a cache namespace. Both datasets carry the same format settings —
     * `schema_resolution` and `file_sort_by` are part of the cache key, so anything else gives them separate
     * entries and tests nothing — and differ only in their mapping: one infers, and reads every part at the
     * anchor's three columns, dropping the wider part's rows; the other declares all four, binds by name — so
     * the file's own header bounds its rows — keeps them, and its count is licensed as the files' physical one.
     * <p>
     * The narrow read's warm {@code COUNT(*)} must equal its own cold one whichever read measured the files
     * first. A licensed count is the file's number; it is not the narrow read's number, and the narrow read is
     * what is being answered.
     */
    public void testALicensedCountDoesNotAnswerForAReadThatDropsWiderRows() throws Exception {
        Map<String, Object> shared = Map.of(
            "format",
            "csv",
            "error_mode",
            "null_field",
            "schema_resolution",
            "first_file_wins",
            "file_sort_by",
            "name"
        );
        LinkedHashMap<String, DatasetFieldMapping> fourColumns = new LinkedHashMap<>();
        fourColumns.put("id", new DatasetFieldMapping("integer", null));
        fourColumns.put("color", new DatasetFieldMapping("keyword", null));
        fourColumns.put("value", new DatasetFieldMapping("integer", null));
        fourColumns.put("extra", new DatasetFieldMapping("keyword", null));

        Path narrowFirst = writeRaggedCorpus();
        Path widerFirst = writeRaggedCorpus();

        // What the narrow read counts on its own, with nothing warm: the answer both orders below must keep.
        long narrowCount = raggedCount(registerDataset("ragged_baseline_csv", globUri(narrowFirst, "*.csv"), shared));
        assertThat("the premise: the narrow read must drop the wider part's rows", narrowCount, equalTo(2L));

        // Order one: the narrow read measured first, then a wider read of the same files licenses their physical
        // counts into the entries the two share.
        String wideAfter = registerStrictDataset("ragged_wide_after", globUri(narrowFirst, "*.csv"), fourColumns, shared);
        assertThat("the wider read keeps every row", raggedCount(wideAfter), equalTo(5L));
        assertThat(raggedCount(registerDataset("ragged_narrow_after", globUri(narrowFirst, "*.csv"), shared)), equalTo(narrowCount));
        assertThat(raggedCount(registerDataset("ragged_narrow_after2", globUri(narrowFirst, "*.csv"), shared)), equalTo(narrowCount));

        // Order two: the wider read fills the entries first, before the narrow read has measured anything.
        String wideFirst = registerStrictDataset("ragged_wide_first", globUri(widerFirst, "*.csv"), fourColumns, shared);
        assertThat("the wider read keeps every row", raggedCount(wideFirst), equalTo(5L));
        assertThat(raggedCount(registerDataset("ragged_narrow_last", globUri(widerFirst, "*.csv"), shared)), equalTo(narrowCount));
        assertThat(raggedCount(registerDataset("ragged_narrow_last2", globUri(widerFirst, "*.csv"), shared)), equalTo(narrowCount));
    }

    private long raggedCount(String dataset) {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"), TimeValue.timeValueMinutes(5))) {
            return (Long) response.response().column(0).iterator().next();
        }
    }

    /** Two parts, the second a column wider than the first, so the anchor's schema cannot bound its rows. */
    private Path writeRaggedCorpus() throws IOException {
        Path dir = createTempDir();
        Files.writeString(dir.resolve("part-00.csv"), "id,color,value\n1,red,10\n2,blue,20\n", StandardCharsets.UTF_8);
        Files.writeString(
            dir.resolve("part-01.csv"),
            "id,color,value,extra\n3,green,30,x\n4,black,40,y\n5,white,50,z\n",
            StandardCharsets.UTF_8
        );
        return dir;
    }

    /**
     * Under first_file_wins the anchor types {@code color} as an integer, so part {@link #MIXED_PART}'s letter cell is
     * null-filled: that column lost a cell in that part. {@code value} lost nothing in any part, so its warm MIN/MAX
     * must be served even though a sibling column of the same file was damaged.
     */
    public void testCsvHeterogeneousCorpusWarmMinMaxServedOnUntouchedColumnFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = writeCsvCorpus(dir, true);
        String dataset = registerDataset("het_ffw_minmax_csv", globUri(dir, "*.csv"), nullFieldSettings("first_file_wins"));
        String minMaxQuery = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("cold MIN/MAX reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("warm MIN/MAX over a column no part damaged must be served", response.documentsFound(), equalTo(0L));
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // The same heterogeneous shape in NDJSON. The crossing rules are format-independent, but the identity that
    // feeds them is stamped by each reader, so nothing before this exercised NDJSON's end to end.
    // ------------------------------------------------------------------------------------------------------------

    private static final int NDJSON_FILE_COUNT = 12;
    private static final int NDJSON_ROWS_PER_FILE = 4_000;

    /**
     * Parts that do not all infer the same schema, in the two ways NDJSON produces: {@code color} holds a string
     * in one part and a number everywhere else, and {@code order_id} is absent from a run of parts, which is
     * NDJSON's analogue of CSV's blank cell — there is no empty cell, a key is simply not there.
     */
    private static long writeNdjsonCorpus(Path dir) throws IOException {
        long total = 0;
        for (int f = 0; f < NDJSON_FILE_COUNT; f++) {
            boolean sparse = f >= 1 && f <= 3;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < NDJSON_ROWS_PER_FILE; i++) {
                long v = total + i;
                sb.append("{\"id\":").append(v).append(',');
                sb.append("\"color\":").append(f == 5 && i == 10 ? "\"g\"" : Long.toString(v % 7)).append(',');
                if (sparse == false) {
                    sb.append("\"order_id\":").append(v % 1000).append(',');
                }
                sb.append("\"value\":").append(v).append("}\n");
            }
            Files.writeString(dir.resolve(String.format(Locale.ROOT, "part-%02d.ndjson", f)), sb.toString(), StandardCharsets.UTF_8);
            total += NDJSON_ROWS_PER_FILE;
        }
        return total;
    }

    public void testNdjsonHeterogeneousCorpusWarmCountServedUnderNullFieldFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = writeNdjsonCorpus(dir);
        String dataset = registerDataset(
            "het_ndjson_ffw",
            globUri(dir, "*.ndjson"),
            Map.of("format", "ndjson", "error_mode", "null_field", "schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    @AwaitsFix(bugUrl = "union_by_name retypes per file, and the pinned-column poison is not lifted yet")
    public void testNdjsonHeterogeneousCorpusWarmCountServedUnderNullFieldUnionByName() throws Exception {
        Path dir = createTempDir();
        long total = writeNdjsonCorpus(dir);
        String dataset = registerDataset(
            "het_ndjson_ubn",
            globUri(dir, "*.ndjson"),
            Map.of("format", "ndjson", "error_mode", "null_field", "schema_resolution", "union_by_name")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    /** {@code value} is read the same way in every part, so its extrema must cross even where {@code color} cannot. */
    public void testNdjsonHeterogeneousCorpusWarmMinMaxServedOnUntouchedColumn() throws Exception {
        Path dir = createTempDir();
        long total = writeNdjsonCorpus(dir);
        String dataset = registerDataset(
            "het_ndjson_minmax",
            globUri(dir, "*.ndjson"),
            Map.of("format", "ndjson", "error_mode", "null_field", "schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        String query = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("cold MIN/MAX reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("warm MIN/MAX must be served for a column no part read differently", response.documentsFound(), equalTo(0L));
        }
    }

    private void assertWarmCountShortCircuits(String dataset, long total) {
        String countQuery = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("cold COUNT(*) reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("warm COUNT(*) must be served from the per-file statistics", response.documentsFound(), equalTo(0L));
        }
    }

    private static void assertSingleLong(EsqlQueryResponse response, long expected) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    private static void assertMinMax(EsqlQueryResponse response, long expectedMin, long expectedMax) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expectedMin));
        assertThat(((Number) rows.get(0).get(1)).longValue(), equalTo(expectedMax));
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    /** Writes a CSV file with {@code value} starting at {@code base}; returns rows written. */
    private static long writeCsvFile(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder("id,name,value\n");
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            long v = base + i;
            sb.append(v).append(",row_").append(v).append(',').append(v).append('\n');
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        return ROWS_PER_FILE;
    }

    /** Writes an NDJSON file with {@code value} starting at {@code base}; returns rows written. */
    private static long writeNdjsonFile(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            long v = base + i;
            sb.append("{\"id\":").append(v).append(",\"name\":\"row_").append(v).append("\",\"value\":").append(v).append("}\n");
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        return ROWS_PER_FILE;
    }

    /**
     * The corpus without a letter cell, so a {@code fail_fast} read completes while parts 1..9 still infer
     * {@code keyword} for {@code order_id} and are therefore read at the anchor's schema.
     */
    private static long writeSparseOnlyCsvCorpus(Path dir) throws IOException {
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            boolean sparse = f >= SPARSE_FIRST_PART && f <= SPARSE_LAST_PART;
            StringBuilder sb = new StringBuilder("id,color,order_id,value\n");
            for (int i = 0; i < HET_ROWS_PER_FILE; i++) {
                long v = total + i;
                String orderId = sparse ? "" : Long.toString(v % 1000);
                sb.append(v).append(',').append(v % 7).append(',').append(orderId).append(',').append(v).append('\n');
            }
            Files.writeString(dir.resolve(String.format(Locale.ROOT, "part-%02d.csv", f)), sb.toString(), StandardCharsets.UTF_8);
            total += HET_ROWS_PER_FILE;
        }
        return total;
    }

    /**
     * {@code fail_fast} is the default error mode, and the arms above all register {@code null_field}. Under
     * {@code fail_fast} the producer licenses the physical row count to cross read configurations, so the count was
     * never the problem; the per-column extrema were, and they had nowhere to be filed.
     */
    public void testCsvSparseCorpusWarmMinMaxServedUnderFailFastFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = writeSparseOnlyCsvCorpus(dir);
        String dataset = registerDataset(
            "ff_minmax_csv",
            globUri(dir, "*.csv"),
            Map.of("format", "csv", "error_mode", "fail_fast", "schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        String minMaxQuery = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("cold MIN/MAX reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("warm MIN/MAX under the default error mode must be served", response.documentsFound(), equalTo(0L));
        }
    }

    /** The count under the same settings, which the licence already served and must keep serving. */
    public void testCsvSparseCorpusWarmCountServedUnderFailFastFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = writeSparseOnlyCsvCorpus(dir);
        String dataset = registerDataset(
            "ff_count_csv",
            globUri(dir, "*.csv"),
            Map.of("format", "csv", "error_mode", "fail_fast", "schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    /**
     * {@code skip_row}, the third mode and the one no arm covered. It is lenient, so unlike {@code fail_fast} it
     * licenses nothing to cross a read configuration - under it a row can disappear, which makes even the count a
     * property of the read. The sparse-only corpus is deliberate: it has nothing for {@code skip_row} to drop, so
     * what this measures is the licence behaviour rather than row loss, and the expected totals stay exact.
     */
    @AwaitsFix(
        bugUrl = "under skip_row, resolvesToSkipRow sets dropPinnedRowCount, and "
            + "RunningFileStatsFold.applyPinnedColumns returns null as soon as ANY file is pinned, so the "
            + "whole dataset aggregate is discarded rather than overlaid. strict is served because its "
            + "corpus pins nothing; null_field is served because it overlays instead of dropping. "
            + "Fails identically on main; tracked by elastic/esql-planning#2201"
    )
    public void testCsvSparseCorpusWarmMinMaxServedUnderSkipRowFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = writeSparseOnlyCsvCorpus(dir);
        String dataset = registerDataset(
            "skiprow_minmax_csv",
            globUri(dir, "*.csv"),
            Map.of("format", "csv", "error_mode", "skip_row", "schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        String minMaxQuery = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("cold MIN/MAX reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("warm MIN/MAX under skip_row must be served", response.documentsFound(), equalTo(0L));
        }
    }

    /** The count under {@code skip_row}, which has no licence and so depends entirely on the read-addressed record. */
    @AwaitsFix(
        bugUrl = "under skip_row, resolvesToSkipRow sets dropPinnedRowCount, and "
            + "RunningFileStatsFold.applyPinnedColumns returns null as soon as ANY file is pinned, so the "
            + "whole dataset aggregate is discarded rather than overlaid. strict is served because its "
            + "corpus pins nothing; null_field is served because it overlays instead of dropping. "
            + "Fails identically on main; tracked by elastic/esql-planning#2201"
    )
    public void testCsvSparseCorpusWarmCountServedUnderSkipRowFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = writeSparseOnlyCsvCorpus(dir);
        String dataset = registerDataset(
            "skiprow_count_csv",
            globUri(dir, "*.csv"),
            Map.of("format", "csv", "error_mode", "skip_row", "schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    /**
     * The warm assertions above cannot see a record losing its measurements, and that blindness has hidden a
     * real defect: a warm {@code COUNT(*)} over many files reports zero documents whether every per-file
     * record carried its harvested count or none of them did, because the dataset aggregate answers the count
     * either way. A contribution refused for some files and accepted for others leaves those files cold
     * forever while every value assertion here still passes.
     * <p>
     * Two signals close it. Every per-file record must carry a row count after the cold scan — a partial
     * enrichment shows up as a count below the file count. And the warm query must not increment the
     * dataset-aggregate fallback, which is what tells the two cases apart: unchanged means the per-file
     * records answered, incremented means the fallback did.
     */
    public void testEveryPerFileRecordIsEnrichedAndTheWarmAnswerIsNotTheFallback() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeCsvFile(dir.resolve("part-" + f + ".csv"), total);
        }
        String dataset = registerDataset("multifile_enrichment_csv", globUri(dir, "*.csv"), Map.of());
        ExternalSourceCacheService cacheService = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();

        String countQuery = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
        }

        String marker = dir.getFileName().toString();
        assertThat(
            "every file the cold scan read must carry a harvested row count; fewer means a contribution was "
                + "refused for some files and those files never warm",
            ExternalSourceCacheTestAccess.enrichedPerFileEntries(cacheService, marker),
            equalTo(FILE_COUNT)
        );

        long fallbacksBefore = ExternalSourceCacheTestAccess.datasetAggregateFallbacks(cacheService);
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("the warm count must not scan", response.documentsFound(), equalTo(0L));
        }
        assertThat(
            "the warm count must come from the per-file records, not from the dataset-aggregate fallback",
            ExternalSourceCacheTestAccess.datasetAggregateFallbacks(cacheService),
            equalTo(fallbacksBefore)
        );
    }

    // ---- the error-mode x schema-resolution matrix ----
    //
    // Two axes decide whether a warm aggregate can be served, and until now the arms above varied only one of them:
    // every restored arm registered null_field. The cells below hold the corpus fixed - the sparse-only one, which
    // has nothing for skip_row to drop and nothing for fail_fast to throw on - so the mode and the resolution are
    // the only things that move, and a red cell names which pair it is.

    private static Map<String, Object> matrixSettings(String errorMode, String schemaResolution) {
        return "first_file_wins".equals(schemaResolution)
            ? Map.of("format", "csv", "error_mode", errorMode, "schema_resolution", schemaResolution, "file_sort_by", "name")
            : Map.of("format", "csv", "error_mode", errorMode, "schema_resolution", schemaResolution);
    }

    /**
     * One cell: register a corpus under this pair, then assert the warm aggregate is served.
     * <p>
     * The corpus follows the resolution rather than being held fixed, because {@code strict} is defined over files
     * that agree: registering a divergent corpus under it is refused outright - "column [order_id] is [keyword], in
     * [part-00.csv] it is [integer]; set [schema_resolution] to [union_by_name]" - so a divergent cell there would
     * test the validator, not the warm path. {@code first_file_wins} and {@code union_by_name} get the divergent
     * corpus, which is the only one where a file is read at another file's schema and so the only one where the
     * read-addressed record does any work.
     */
    private void assertWarmMatrixCell(String name, String errorMode, String schemaResolution, boolean minMax) throws Exception {
        Path dir = createTempDir();
        long total = "strict".equals(schemaResolution) ? writeCsvCorpus(dir, false) : writeSparseOnlyCsvCorpus(dir);
        String dataset = registerDataset(name, globUri(dir, "*.csv"), matrixSettings(errorMode, schemaResolution));
        if (minMax == false) {
            assertWarmCountShortCircuits(dataset, total);
            return;
        }
        String query = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("cold MIN/MAX reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat(
                "warm MIN/MAX must be served for [" + errorMode + " x " + schemaResolution + "]",
                response.documentsFound(),
                equalTo(0L)
            );
        }
    }

    public void testMatrixCountFailFastUnionByName() throws Exception {
        assertWarmMatrixCell("m_count_fail_unio", "fail_fast", "union_by_name", false);
    }

    @AwaitsFix(bugUrl = "union_by_name retypes per file, and the pinned-column poison is not lifted yet")
    public void testMatrixMinMaxFailFastUnionByName() throws Exception {
        assertWarmMatrixCell("m_minmax_fail_unio", "fail_fast", "union_by_name", true);
    }

    public void testMatrixCountFailFastStrict() throws Exception {
        assertWarmMatrixCell("m_count_fail_stri", "fail_fast", "strict", false);
    }

    public void testMatrixMinMaxFailFastStrict() throws Exception {
        assertWarmMatrixCell("m_minmax_fail_stri", "fail_fast", "strict", true);
    }

    @AwaitsFix(
        bugUrl = "under skip_row, resolvesToSkipRow sets dropPinnedRowCount, and "
            + "RunningFileStatsFold.applyPinnedColumns returns null as soon as ANY file is pinned, so the "
            + "whole dataset aggregate is discarded rather than overlaid. strict is served because its "
            + "corpus pins nothing; null_field is served because it overlays instead of dropping. "
            + "Fails identically on main; tracked by elastic/esql-planning#2201"
    )
    public void testMatrixCountSkipRowUnionByName() throws Exception {
        assertWarmMatrixCell("m_count_skip_unio", "skip_row", "union_by_name", false);
    }

    @AwaitsFix(
        bugUrl = "under skip_row, resolvesToSkipRow sets dropPinnedRowCount, and "
            + "RunningFileStatsFold.applyPinnedColumns returns null as soon as ANY file is pinned, so the "
            + "whole dataset aggregate is discarded rather than overlaid. strict is served because its "
            + "corpus pins nothing; null_field is served because it overlays instead of dropping. "
            + "Fails identically on main; tracked by elastic/esql-planning#2201"
    )
    public void testMatrixMinMaxSkipRowUnionByName() throws Exception {
        assertWarmMatrixCell("m_minmax_skip_unio", "skip_row", "union_by_name", true);
    }

    public void testMatrixCountSkipRowStrict() throws Exception {
        assertWarmMatrixCell("m_count_skip_stri", "skip_row", "strict", false);
    }

    public void testMatrixMinMaxSkipRowStrict() throws Exception {
        assertWarmMatrixCell("m_minmax_skip_stri", "skip_row", "strict", true);
    }

    @AwaitsFix(bugUrl = "union_by_name retypes per file, and the pinned-column poison is not lifted yet")
    public void testMatrixCountNullFieldUnionByName() throws Exception {
        assertWarmMatrixCell("m_count_null_unio", "null_field", "union_by_name", false);
    }

    @AwaitsFix(bugUrl = "union_by_name retypes per file, and the pinned-column poison is not lifted yet")
    public void testMatrixMinMaxNullFieldUnionByName() throws Exception {
        assertWarmMatrixCell("m_minmax_null_unio", "null_field", "union_by_name", true);
    }

    public void testMatrixMinMaxNullFieldStrict() throws Exception {
        assertWarmMatrixCell("m_minmax_null_stri", "null_field", "strict", true);
    }

    public void testMatrixMinMaxNullFieldFirstFileWins() throws Exception {
        assertWarmMatrixCell("m_minmax_null_firs", "null_field", "first_file_wins", true);
    }

    public void testMatrixMinMaxFailFastFirstFileWins() throws Exception {
        assertWarmMatrixCell("m_minmax_fail_firs", "fail_fast", "first_file_wins", true);
    }

    public void testMatrixCountFailFastFirstFileWins() throws Exception {
        assertWarmMatrixCell("m_count_fail_firs", "fail_fast", "first_file_wins", false);
    }

    public void testMatrixCountNullFieldFirstFileWins() throws Exception {
        assertWarmMatrixCell("m_count_null_firs", "null_field", "first_file_wins", false);
    }

    public void testMatrixCountNullFieldStrict() throws Exception {
        assertWarmMatrixCell("m_count_null_stri", "null_field", "strict", false);
    }

    @AwaitsFix(
        bugUrl = "under skip_row, resolvesToSkipRow sets dropPinnedRowCount, and "
            + "RunningFileStatsFold.applyPinnedColumns returns null as soon as ANY file is pinned, so the "
            + "whole dataset aggregate is discarded rather than overlaid. strict is served because its "
            + "corpus pins nothing; null_field is served because it overlays instead of dropping. "
            + "Fails identically on main; tracked by elastic/esql-planning#2201"
    )
    public void testMatrixMinMaxSkipRowFirstFileWins() throws Exception {
        assertWarmMatrixCell("m_minmax_skip_firs", "skip_row", "first_file_wins", true);
    }

    @AwaitsFix(
        bugUrl = "under skip_row, resolvesToSkipRow sets dropPinnedRowCount, and "
            + "RunningFileStatsFold.applyPinnedColumns returns null as soon as ANY file is pinned, so the "
            + "whole dataset aggregate is discarded rather than overlaid. strict is served because its "
            + "corpus pins nothing; null_field is served because it overlays instead of dropping. "
            + "Fails identically on main; tracked by elastic/esql-planning#2201"
    )
    public void testMatrixCountSkipRowFirstFileWins() throws Exception {
        assertWarmMatrixCell("m_count_skip_firs", "skip_row", "first_file_wins", false);
    }
}
