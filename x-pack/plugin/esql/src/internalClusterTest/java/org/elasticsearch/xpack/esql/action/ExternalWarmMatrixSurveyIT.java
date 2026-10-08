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
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheTestAccess;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * A SURVEY, not a gate: for every combination of format, corpus shape, schema resolution, schema declaration and
 * error mode, run an aggregate cold and then warm and record whether the warm run read any rows. Nothing here
 * asserts that a cell warms — the point is to produce the picture of which cells do, so the ones that do not can
 * be root-caused. The only assertion is that the answer is correct, so a cold cell is distinguishable from a
 * wrong one.
 * <p>
 * Corpus is deliberately small (6 files, 2,000 rows) so the full cross is runnable. The sparse column is blank in
 * EVERY row of its parts, so inference falls to the keyword default regardless of the inference window size.
 */
public class ExternalWarmMatrixSurveyIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 6;
    private static final int ROWS_PER_FILE = 2_000;
    private static final int SPARSE_FIRST = 1;
    private static final int SPARSE_LAST = 3;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("esql.external.cache.stripe.size", "64kb")
            .build();
    }

    /**
     * Pins every query to one coordinator. The reconciled schema cache is per-coordinator, so without this the
     * cold scan and the warm re-read land on different nodes and EVERY cell reports RESCAN - which is what the
     * first run of this survey did, measuring the harness instead of the product.
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    /**
     * DIAGNOSTIC, not a survey cell. After a cold declared COUNT(*) on a text dataset, is the statistics store
     * EMPTY or NON-EMPTY? The two answers have different fixes and the survey cannot tell them apart:
     *   empty     -> the harvest was never filed (the reconcile's SchemaCacheEntry dependency drops it)
     *   non-empty -> it WAS filed, at an address the serve does not look up (a labelling mismatch, #2201 item 4)
     * Run against inferred as the control: inferred warms, so its store must be non-empty and enriched.
     */
    public void testWhereTheDeclaredHarvestGoes() throws Exception {
        for (String declaration : List.of("inferred", "declared", "overlay")) {
            Path dir = createTempDir();
            long total = writeTextCorpus(dir, "csv", "homogeneous");
            String marker = dir.getFileName().toString();
            String dataset = register(
                "diag_" + declaration,
                globUri(dir, "*.csv"),
                declaration,
                settingsFor("csv", "null_field", "first_file_wins")
            );
            ExternalSourceCacheService svc = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
                .cacheService();
            long statsBefore = ExternalSourceCacheTestAccess.retainedStatisticsWeightBytes(svc);
            long cold;
            try (
                var r = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)").profile(true), TimeValue.timeValueMinutes(2))
            ) {
                cold = r.documentsFound();
            }
            long statsAfter = ExternalSourceCacheTestAccess.retainedStatisticsWeightBytes(svc);
            long schemaAfter = ExternalSourceCacheTestAccess.retainedSchemaWeightBytes(svc);
            int enriched = ExternalSourceCacheTestAccess.enrichedPerFileEntries(svc, marker);
            long warm;
            try (
                var r = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)").profile(true), TimeValue.timeValueMinutes(2))
            ) {
                warm = r.documentsFound();
            }
            logger.info(
                "DIAG[{}] rows={} cold={} warm={} statisticsWeight {}->{} schemaWeight={} enrichedPerFileEntries={}",
                declaration,
                total,
                cold,
                warm,
                statsBefore,
                statsAfter,
                schemaAfter,
                enriched
            );
        }
    }

    /** One surveyed cell and what happened. */
    private record Cell(
        String format,
        String corpus,
        String resolution,
        String declaration,
        String errorMode,
        String aggregate,
        String outcome,
        String detail
    ) {
        String row() {
            return String.format(
                Locale.ROOT,
                "| %-7s | %-11s | %-15s | %-9s | %-10s | %-7s | %-9s | %s",
                format,
                corpus,
                resolution,
                declaration,
                errorMode,
                aggregate,
                outcome,
                detail
            );
        }
    }

    private final List<Cell> cells = new ArrayList<>();

    /**
     * Corpus shapes. Parts 0, 4 and 5 always carry {@code id,color,order_id,value}, all integer-valued; parts 1..3
     * ("odd") differ per shape:
     * <ul>
     *   <li>{@code homogeneous} — identical to the rest. The control.</li>
     *   <li>{@code presence} — {@code order_id} ABSENT (3-column header / omitted key / 3-field footer). A strict
     *       subset: the union equals part 0's schema.</li>
     *   <li>{@code conflict} — {@code order_id} present but keyword-typed (blank cell in every row / quoted number /
     *       binary(UTF8) footer field). Reconciles only by a decision (keyword fallback).</li>
     *   <li>{@code widen} — {@code order_id} present and integer-typed where the rest are long-valued (small values /
     *       int32 footer field vs. values above 2^31 / int64). Reconciles by safe widening, no decision needed.</li>
     *   <li>{@code misaligned} — the odd parts carry {@code ship_id} instead of {@code order_id}: neither column set
     *       is a subset of the other and the union is wider than every file.</li>
     *   <li>{@code order} — same names and types, header order {@code id,value,order_id,color}. Isolates
     *       positional vs. by-name binding; ndjson keys are unordered by the reader so that arm is the same corpus
     *       with reordered keys and is reported as such.</li>
     * </ul>
     */
    private static final List<String> CORPORA = List.of("homogeneous", "presence", "conflict", "widen", "misaligned", "order");

    private static final List<String> CANONICAL_COLUMNS = List.of("id", "color", "order_id", "value");

    /** Column names of one part under {@code corpus}. */
    private static List<String> columnsOf(String corpus, boolean odd) {
        if (odd == false) {
            return CANONICAL_COLUMNS;
        }
        return switch (corpus) {
            case "presence" -> List.of("id", "color", "value");
            case "misaligned" -> List.of("id", "color", "ship_id", "value");
            case "order" -> List.of("id", "value", "order_id", "color");
            default -> CANONICAL_COLUMNS;
        };
    }

    /**
     * The order_id value of row {@code v}: small (integer-inferred) everywhere, except the non-odd parts of
     * {@code widen}, which carry values above 2^31 so they infer long and the odd parts widen into them.
     */
    private static long orderIdOf(String corpus, boolean odd, long v) {
        return "widen".equals(corpus) && odd == false ? 3_000_000_000L + v % 1000 : v % 1000;
    }

    public void testSurveyTextFormats() throws Exception {
        for (String format : List.of("csv", "tsv", "ndjson")) {
            for (String corpus : CORPORA) {
                for (String resolution : List.of("first_file_wins", "union_by_name", "strict")) {
                    for (String declaration : List.of("inferred", "declared", "overlay")) {
                        for (String errorMode : List.of("fail_fast", "null_field", "skip_row")) {
                            for (String aggregate : List.of("count", "minmax")) {
                                survey(format, corpus, resolution, declaration, errorMode, aggregate);
                            }
                        }
                    }
                }
            }
        }
        report();
    }

    private void survey(String format, String corpus, String resolution, String declaration, String errorMode, String aggregate)
        throws Exception {
        String name = ("s_"
            + format
            + "_"
            + corpus.charAt(0)
            + "_"
            + resolution.charAt(0)
            + "_"
            + declaration.charAt(0)
            + "_"
            + errorMode.charAt(0)
            + "_"
            + aggregate.charAt(0)).toLowerCase(Locale.ROOT);
        long total;
        String dataset;
        try {
            Path dir = createTempDir();
            total = writeTextCorpus(dir, format, corpus);
            Map<String, Object> settings = settingsFor(format, errorMode, resolution);
            dataset = register(name, globUri(dir, "*." + extensionOf(format)), declaration, settings);
        } catch (Exception e) {
            cells.add(new Cell(format, corpus, resolution, declaration, errorMode, aggregate, "REFUSED", shortMessage(e)));
            return;
        }
        runCell(format, corpus, resolution, declaration, errorMode, aggregate, dataset, total);
    }

    private void runCell(
        String format,
        String corpus,
        String resolution,
        String declaration,
        String errorMode,
        String aggregate,
        String dataset,
        long total
    ) {
        String query = "count".equals(aggregate)
            ? "FROM " + dataset + " | STATS c = COUNT(*)"
            : "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        // The expected answer: every corpus shape keeps id == value == row ordinal, so COUNT(*) is the row total and
        // MIN/MAX(value) are 0 and total-1 whatever happened to order_id. A served answer that disagrees is recorded
        // as WRONG on the cell rather than thrown, so one wrong cell does not hide the rest of the table.
        List<Object> expected = "count".equals(aggregate) ? List.of(total) : List.of(0L, total - 1);
        long cold;
        String wrong = "";
        try (var r = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            cold = r.documentsFound();
            wrong += mismatch("cold", expected, r);
        } catch (Exception e) {
            cells.add(new Cell(format, corpus, resolution, declaration, errorMode, aggregate, "QUERY_ERR", shortMessage(e)));
            return;
        }
        long warm;
        try (var r = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            warm = r.documentsFound();
            wrong += mismatch("warm", expected, r);
        } catch (Exception e) {
            cells.add(new Cell(format, corpus, resolution, declaration, errorMode, aggregate, "QUERY_ERR", shortMessage(e)));
            return;
        }
        String outcome = warm == 0 ? "WARM" : (warm >= cold ? "RESCAN" : "PARTIAL");
        cells.add(
            new Cell(
                format,
                corpus,
                resolution,
                declaration,
                errorMode,
                aggregate,
                outcome,
                "cold=" + cold + " warm=" + warm + " rows=" + total + wrong
            )
        );
    }

    /** Empty when the one-row answer equals {@code expected} (compared as longs), else a WRONG marker naming both. */
    private static String mismatch(String run, List<Object> expected, EsqlQueryResponse r) {
        List<List<Object>> rows = getValuesList(r);
        List<Object> actual = rows.size() == 1 ? rows.get(0) : List.of();
        boolean same = actual.size() == expected.size();
        for (int i = 0; same && i < expected.size(); i++) {
            same = actual.get(i) instanceof Number n && n.longValue() == ((Number) expected.get(i)).longValue();
        }
        return same ? "" : " WRONG(" + run + " expected=" + expected + " actual=" + rows + ")";
    }

    /**
     * BINDING PROBE, not a warm measurement: whether a file whose columns are not where the anchor's are gets read
     * by position or by name. {@code order} swaps {@code color} and {@code value} in parts 1..3, so a positional read
     * reports {@code MAX(color)} as the largest {@code value} (11999) instead of 6. {@code misaligned} puts
     * {@code ship_id} where the anchor has {@code order_id}, so a positional read counts 12000 non-null
     * {@code order_id} instead of the 6000 the three files that carry it hold.
     */
    public void testBindingProbe() throws Exception {
        StringBuilder sb = new StringBuilder("\n===== BINDING PROBE =====\n");
        for (String format : List.of("csv", "tsv", "ndjson", "parquet")) {
            for (String corpus : List.of("homogeneous", "presence", "conflict", "order", "misaligned")) {
                for (String resolution : List.of("first_file_wins", "union_by_name")) {
                    for (String declaration : List.of("inferred", "declared", "overlay")) {
                        String name = ("b_" + format + "_" + corpus.charAt(0) + "_" + resolution.charAt(0) + "_" + declaration.charAt(0))
                            .toLowerCase(Locale.ROOT);
                        Path dir = createTempDir();
                        boolean parquet = "parquet".equals(format);
                        long total = parquet ? writeParquetCorpus(dir, corpus) : writeTextCorpus(dir, format, corpus);
                        String query;
                        long expected;
                        if ("order".equals(corpus)) {
                            query = " | STATS m = MAX(color)";
                            expected = 6;
                        } else {
                            // Only `presence` (odd parts omit order_id) and `misaligned` (odd parts carry ship_id
                            // instead) leave order_id in just 3 of the 6 parts. homogeneous, conflict and widen keep
                            // it in every part, so the correct count is every row. Getting this wrong in either
                            // direction both invents defects and blesses real ones.
                            query = " | STATS c = COUNT(order_id)";
                            expected = ("presence".equals(corpus) || "misaligned".equals(corpus)) ? total / 2 : total;
                        }
                        String outcome;
                        try {
                            String dataset = register(
                                name,
                                globUri(dir, "*." + (parquet ? "parquet" : extensionOf(format))),
                                declaration,
                                settingsFor(format, "null_field", resolution)
                            );
                            try (var r = run(syncEsqlQueryRequest("FROM " + dataset + query), TimeValue.timeValueMinutes(2))) {
                                List<List<Object>> rows = getValuesList(r);
                                Object actual = rows.size() == 1 && rows.get(0).size() == 1 ? rows.get(0).get(0) : rows;
                                boolean ok = actual instanceof Number n && n.longValue() == expected;
                                outcome = (ok ? "OK   " : "WRONG") + " expected=" + expected + " actual=" + actual;
                            }
                        } catch (Exception e) {
                            outcome = "ERR   " + shortMessage(e);
                        }
                        sb.append(
                            String.format(
                                Locale.ROOT,
                                "| %-7s | %-10s | %-15s | %-8s | %s%n",
                                format,
                                corpus,
                                resolution,
                                declaration,
                                outcome
                            )
                        );
                    }
                }
            }
        }
        logger.info(sb.toString());
    }

    private String register(String name, String uri, String declaration, Map<String, Object> settings) {
        return switch (declaration) {
            case "inferred" -> registerDataset(name, uri, settings);
            case "declared" -> registerStrictDataset(name, uri, declaredProperties(), settings);
            case "overlay" -> registerNonStrictDataset(name, uri, declaredProperties(), settings);
            default -> throw new IllegalArgumentException(declaration);
        };
    }

    /** The declared columns: the whole schema for a strict mapping, an overlay for a non-strict one. */
    private static LinkedHashMap<String, DatasetFieldMapping> declaredProperties() {
        LinkedHashMap<String, DatasetFieldMapping> p = new LinkedHashMap<>();
        p.put("id", new DatasetFieldMapping("long", null));
        p.put("color", new DatasetFieldMapping("keyword", null));
        p.put("order_id", new DatasetFieldMapping("keyword", null));
        p.put("value", new DatasetFieldMapping("long", null));
        return p;
    }

    private static Map<String, Object> settingsFor(String format, String errorMode, String resolution) {
        return "first_file_wins".equals(resolution)
            ? Map.of("format", format, "error_mode", errorMode, "schema_resolution", resolution, "file_sort_by", "name")
            : Map.of("format", format, "error_mode", errorMode, "schema_resolution", resolution);
    }

    private static String extensionOf(String format) {
        return "ndjson".equals(format) ? "ndjson" : format;
    }

    private static long writeTextCorpus(Path dir, String format, String corpus) throws IOException {
        long total = 0;
        char delim = "tsv".equals(format) ? '\t' : ',';
        boolean ndjson = "ndjson".equals(format);
        for (int f = 0; f < FILE_COUNT; f++) {
            boolean odd = f >= SPARSE_FIRST && f <= SPARSE_LAST;
            boolean conflict = "conflict".equals(corpus) && odd;
            List<String> columns = columnsOf(corpus, odd);
            StringBuilder sb = new StringBuilder();
            if (ndjson == false) {
                sb.append(String.join(String.valueOf(delim), columns)).append('\n');
            }
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                long v = total + i;
                if (ndjson) {
                    sb.append('{');
                }
                for (int c = 0; c < columns.size(); c++) {
                    String column = columns.get(c);
                    String cell = switch (column) {
                        case "id" -> Long.toString(v);
                        case "color" -> Long.toString(v % 7);
                        case "value" -> Long.toString(v);
                        case "ship_id" -> Long.toString(v % 1000);
                        case "order_id" -> conflict ? (ndjson ? "\"" + (v % 1000) + "\"" : "") : Long.toString(orderIdOf(corpus, odd, v));
                        default -> throw new IllegalStateException(column);
                    };
                    if (c > 0) {
                        sb.append(ndjson ? ',' : delim);
                    }
                    if (ndjson) {
                        sb.append('"').append(column).append("\":");
                    }
                    sb.append(cell);
                }
                sb.append(ndjson ? "}\n" : "\n");
            }
            Files.writeString(
                dir.resolve(String.format(Locale.ROOT, "part-%02d.%s", f, extensionOf(format))),
                sb.toString(),
                StandardCharsets.UTF_8
            );
            total += ROWS_PER_FILE;
        }
        return total;
    }

    /**
     * Parquet. Divergence here cannot be an inference outcome - a Parquet file carries its schema in the footer -
     * so the divergent corpus gives parts 1..3 a {@code binary} (UTF8) order_id where the rest have {@code int64}.
     * That is a genuine physical-schema disagreement between files, the columnar analogue of the text corpus's
     * "blank in the early parts". error_mode is crossed anyway: it is a text concept, and whether it is accepted,
     * ignored or rejected for a columnar format is itself part of the picture.
     */
    public void testSurveyParquet() throws Exception {
        for (String corpus : CORPORA) {
            for (String resolution : List.of("first_file_wins", "union_by_name", "strict")) {
                for (String declaration : List.of("inferred", "declared", "overlay")) {
                    for (String errorMode : List.of("fail_fast", "null_field", "skip_row")) {
                        for (String aggregate : List.of("count", "minmax")) {
                            surveyParquet(corpus, resolution, declaration, errorMode, aggregate);
                        }
                    }
                }
            }
        }
        report();
    }

    private void surveyParquet(String corpus, String resolution, String declaration, String errorMode, String aggregate) throws Exception {
        String name = ("s_pq_"
            + corpus.charAt(0)
            + "_"
            + resolution.charAt(0)
            + "_"
            + declaration.charAt(0)
            + "_"
            + errorMode.charAt(0)
            + "_"
            + aggregate.charAt(0)).toLowerCase(Locale.ROOT);
        long total;
        String dataset;
        try {
            Path dir = createTempDir();
            total = writeParquetCorpus(dir, corpus);
            dataset = register(name, globUri(dir, "*.parquet"), declaration, settingsFor("parquet", errorMode, resolution));
        } catch (Exception e) {
            cells.add(new Cell("parquet", corpus, resolution, declaration, errorMode, aggregate, "REFUSED", shortMessage(e)));
            return;
        }
        runCell("parquet", corpus, resolution, declaration, errorMode, aggregate, dataset, total);
    }

    private static long writeParquetCorpus(Path dir, String corpus) throws IOException {
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            boolean odd = f >= SPARSE_FIRST && f <= SPARSE_LAST;
            boolean conflict = "conflict".equals(corpus) && odd;
            boolean narrow = "widen".equals(corpus) && odd;
            List<String> columns = columnsOf(corpus, odd);
            StringBuilder schema = new StringBuilder("message test { ");
            for (String column : columns) {
                String type = "order_id".equals(column) && conflict ? "binary " + column + " (UTF8)"
                    : "order_id".equals(column) && narrow ? "int32 " + column
                    : "int64 " + column;
                schema.append("required ").append(type).append("; ");
            }
            schema.append('}');
            final long base = total;
            writeParquet(
                dir.resolve(String.format(Locale.ROOT, "part-%02d.parquet", f)),
                schema.toString(),
                ROWS_PER_FILE,
                1024,
                (g, i) -> {
                    long v = base + i;
                    for (String column : columns) {
                        switch (column) {
                            case "id", "value" -> g.add(column, v);
                            case "color" -> g.add(column, v % 7);
                            case "ship_id" -> g.add(column, v % 1000);
                            case "order_id" -> {
                                if (conflict) {
                                    g.add(column, Long.toString(v % 1000));
                                } else if (narrow) {
                                    g.add(column, (int) (v % 1000));
                                } else {
                                    g.add(column, orderIdOf(corpus, odd, v));
                                }
                            }
                            default -> throw new IllegalStateException(column);
                        }
                    }
                }
            );
            total += ROWS_PER_FILE;
        }
        return total;
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    private static String shortMessage(Exception e) {
        String m = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        m = m.replace('\n', ' ');
        return m.length() > 110 ? m.substring(0, 110) : m;
    }

    private void report() {
        StringBuilder sb = new StringBuilder("\n===== WARM MATRIX SURVEY =====\n");
        sb.append("| format  | corpus      | resolution      | declared  | error_mode | agg     | outcome   | detail\n");
        sb.append("|---------|-------------|-----------------|-----------|------------|---------|-----------|-------\n");
        for (Cell c : cells) {
            sb.append(c.row()).append('\n');
        }
        long warm = cells.stream().filter(c -> "WARM".equals(c.outcome())).count();
        long rescan = cells.stream().filter(c -> "RESCAN".equals(c.outcome())).count();
        long refused = cells.stream().filter(c -> "REFUSED".equals(c.outcome())).count();
        long err = cells.stream().filter(c -> "QUERY_ERR".equals(c.outcome())).count();
        sb.append(
            String.format(
                Locale.ROOT,
                "===== cells=%d WARM=%d RESCAN=%d REFUSED=%d QUERY_ERR=%d =====%n",
                cells.size(),
                warm,
                rescan,
                refused,
                err
            )
        );
        logger.info(sb.toString());

        // POSITIVE CONTROL, per format run. A table where nothing warms cannot be told apart from a
        // misconfigured harness - which is what the first run of this survey was (no coordinator pinning, every
        // one of 324 cells reported RESCAN). So each run asserts a cell that MUST warm.
        //
        // For the text run the control is csv/homogeneous/strict/inferred/count, which
        // ExternalMultiFileWarmAggregateFoldIT's unmuted testMatrixCount*Strict cells assert warms.
        //
        // For the parquet run there is no equivalent asserted-warming multi-file cell in the tree to borrow, so
        // the control is weaker and deliberately labelled as such: at least one parquet cell must warm. If none
        // does, this does NOT prove parquet never warms - it means the control could not discriminate and the
        // parquet table must be verified another way before it is believed.
        boolean sawCsv = cells.stream().anyMatch(c -> "csv".equals(c.format()));
        if (sawCsv) {
            boolean controlWarms = cells.stream()
                .anyMatch(
                    c -> "csv".equals(c.format())
                        && "homogeneous".equals(c.corpus())
                        && "strict".equals(c.resolution())
                        && "inferred".equals(c.declaration())
                        && "count".equals(c.aggregate())
                        && "WARM".equals(c.outcome())
                );
            if (controlWarms == false) {
                throw new AssertionError(
                    "positive control did not warm: csv/homogeneous/strict/inferred/count must report WARM, so a "
                        + "table where it does not is measuring this harness rather than the product. Table above."
                );
            }
        } else if (cells.stream().noneMatch(c -> "WARM".equals(c.outcome()))) {
            logger.warn(
                "WEAK CONTROL UNSATISFIED: no cell in this run warmed, so this table cannot be distinguished from a "
                    + "misconfigured harness. Verify before believing it."
            );
        }
    }
}
