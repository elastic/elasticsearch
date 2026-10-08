/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.example.data.Group;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheTestAccess;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * COLD/WARM matrix for external-dataset aggregates. One cell is a
 * {@code (format, arity, corpus shape, schema supply, schema_resolution, error_mode)} tuple; inside a cell every
 * aggregate shape is run twice over the same unchanged corpus and the second pass is classified against the first.
 *
 * <p>This pass measures SERVING ONLY. It does not check that the served answer is right: a {@code SERVED} cell here
 * means "the second pass emitted no rows", not "the second pass was correct". Correctness is a separate pass.
 *
 * <p>Three things make the reading real rather than an artefact, and all three have burned prior harnesses:
 * the coordinator is pinned ({@link ClusterScope} one data node plus {@link #run} on the master), every cell gets
 * its own temp directory (the cache's {@code DefinitionVersion} folds resource and settings but not the mapping, so
 * cells over one directory with equal settings share addresses), and the controls at
 * {@link #testControls()} must show both a cell that warms and a cell that does not.
 *
 * <p>Parquet is read differently: {@code PushStatsToExternalSource} folds {@code COUNT(*)} from footers on the FIRST
 * query, so pass 1 already emits zero rows and a rows-based reading says nothing. Every cell therefore also records
 * the coordinator cache-counter deltas across each pass, and the Parquet rows are read off those.
 *
 * <p>Results are emitted one line per (cell, aggregate) at INFO with the {@value #ROW_PREFIX} prefix; the gradle
 * test listener writes every suite's stdout to {@code build/test-results/internalClusterTest/output/}, which is
 * where the matrix is harvested from.
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalWarmColdMatrixIT extends AbstractExternalDataSourceIT {

    static final String ROW_PREFIX = "WARMCELL|";

    private static final int MULTI_FILE_COUNT = 6;
    private static final int ROWS_PER_FILE = 200;
    /** Parts carrying the shape's divergence. The rest are canonical. */
    private static final List<Integer> ODD_PARTS = List.of(1, 2, 3);
    /** Above {@link Integer#MAX_VALUE}, so a column holding it infers long and a declared integer cannot hold it. */
    private static final long WIDE_BASE = 3_000_000_000L;

    private int cellSeq = 0;
    /**
     * One temp root per test method, subdivided per cell. {@code createTempDir()} gives up after a few thousand
     * calls in one JVM ("Failed to get a temporary name too many times"), and a cross this wide makes far more
     * than that, so calling it per cell silently turned most of a group into registration refusals.
     */
    private Path cellRoot;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Matches the fold IT's geometry. No pragma reaches the request, so every harvest is whole-file.
            .put("esql.external.cache.stripe.size", "64kb")
            // The default budget is 0.5% of a test heap, which holds a few hundred schema records. A cross this
            // wide evicts constantly at that size, and an evicted record re-scans - indistinguishable from a rail
            // that never warms. Sized so one cell's records can never be the thing that got evicted.
            .put("esql.external.cache.size", "30%")
            .build();
    }

    /**
     * The schema cache is per coordinator, so pass 1 and pass 2 must hit the same node. Without this every cell
     * reads as a re-scan.
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Dimensions
    // ------------------------------------------------------------------------------------------------------

    enum Fmt {
        CSV("csv", "csv"),
        TSV("tsv", "tsv"),
        NDJSON("ndjson", "ndjson"),
        PARQUET("parquet", "parquet");

        final String setting;
        final String ext;

        Fmt(String setting, String ext) {
            this.setting = setting;
            this.ext = ext;
        }
    }

    /**
     * Corpus shapes, in the OWNER's vocabulary. {@code mismatched} is different column SETS; {@code misaligned} is
     * the same columns in a different header ORDER; {@code conflict} is a real irreconcilable type difference
     * (keyword against integer), not a blank column.
     */
    enum Shape {
        HOMOGENEOUS,
        MISMATCHED,
        MISALIGNED,
        ABSENT,
        CONFLICT,
        WIDEN,
        // single-file: the disagreement is declaration-against-file, not file-against-file
        EXACT,
        DECL_ABSENT,
        DECL_NARROWER,
        DECL_CONFLICT,
        DECL_WIDEN,
        DECL_REORDERED
    }

    enum Supply {
        /** No mapping at all. */
        INFERRED,
        /** {@code dynamic:false} with the canonical declaration. */
        DECLARED,
        /** {@code dynamic:false}, no {@code schema_resolution} key, registered through the pass-through {@code test} type. */
        DECLARED_ONLY_TEST,
        /** {@code dynamic:false}, no {@code schema_resolution} key, registered through {@code local} so PUT stores a default. */
        DECLARED_ONLY_LOCAL,
        /** {@code dynamic:true} declaring what part 0 infers. */
        OVERLAY_IDENTITY,
        /** {@code dynamic:true} retyping order_id to keyword. */
        OVERLAY_RETYPE
    }

    record Agg(String id, String projection) {}

    private static final List<Agg> AGGS = List.of(
        new Agg("count_star", "COUNT(*)"),
        new Agg("count_value", "COUNT(value)"),
        new Agg("count_order_id", "COUNT(order_id)"),
        new Agg("minmax_value", "MIN(value), MAX(value)"),
        new Agg("minmax_label", "MIN(label), MAX(label)"),
        new Agg("sum_avg_value", "SUM(value), AVG(value)")
    );

    record Cell(Fmt fmt, int fileCount, Shape shape, Supply supply, String resolution, String errorMode) {
        String id() {
            return fmt.setting
                + "/"
                + (fileCount == 1 ? "single" : "multi")
                + "/"
                + shape.name().toLowerCase(Locale.ROOT)
                + "/"
                + supply.name().toLowerCase(Locale.ROOT)
                + "/"
                + (resolution == null ? "none" : resolution)
                + "/"
                + errorMode;
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Corpus. Canonical columns, in physical order: id, color, label, order_id, value.
    // id == value == global ordinal; color = v % 7 (numeric); label = "r" + v % 7 (keyword); order_id = v % 1000.
    // ------------------------------------------------------------------------------------------------------

    /** One physical part: its header in file order, which columns are numeric, and its rows as written text. */
    record Part(String name, List<String> columns, List<Boolean> numeric, List<List<String>> rows) {}

    private static List<Part> corpus(Shape shape, int fileCount) {
        List<Part> parts = new ArrayList<>();
        long v = 0;
        for (int f = 0; f < fileCount; f++) {
            boolean odd = fileCount > 1 && ODD_PARTS.contains(f);
            List<String> cols = new ArrayList<>(List.of("id", "color", "label", "order_id", "value"));
            List<Boolean> num = new ArrayList<>(List.of(true, true, false, true, true));
            switch (shape) {
                case MISMATCHED -> {
                    if (odd) {
                        cols.set(3, "ship_id");
                    }
                }
                case MISALIGNED -> {
                    if (odd) {
                        cols = new ArrayList<>(List.of("id", "value", "order_id", "label", "color"));
                        num = new ArrayList<>(List.of(true, true, true, false, true));
                    }
                }
                case ABSENT -> {
                    if (odd) {
                        cols.remove("order_id");
                        num = new ArrayList<>(List.of(true, true, false, true));
                    }
                }
                case CONFLICT -> {
                    // Letters in every row of order_id in the odd parts: keyword against the canonical parts' integer.
                    if (odd) {
                        num.set(3, false);
                    }
                }
                default -> {
                    // HOMOGENEOUS / WIDEN / the single-file shapes all use the canonical header.
                }
            }
            List<List<String>> rows = new ArrayList<>();
            for (int i = 0; i < ROWS_PER_FILE; i++, v++) {
                List<String> row = new ArrayList<>(cols.size());
                for (String col : cols) {
                    row.add(cellText(col, v, shape, odd));
                }
                rows.add(row);
            }
            parts.add(new Part(String.format(Locale.ROOT, "part-%02d", f), cols, num, rows));
        }
        return parts;
    }

    private static String cellText(String col, long v, Shape shape, boolean odd) {
        return switch (col) {
            case "id", "value" -> Long.toString(v);
            case "color" -> Long.toString(v % 7);
            case "label" -> "r" + (v % 7);
            case "ship_id" -> Long.toString(v % 1000);
            case "order_id" -> {
                if (shape == Shape.CONFLICT && odd) {
                    yield "x" + (v % 1000);
                }
                if (shape == Shape.WIDEN && odd == false) {
                    yield Long.toString(WIDE_BASE + (v % 1000));
                }
                yield Long.toString(v % 1000);
            }
            default -> throw new IllegalArgumentException("unknown column [" + col + "]");
        };
    }

    // ------------------------------------------------------------------------------------------------------
    // Fixture writers
    // ------------------------------------------------------------------------------------------------------

    private static void write(Path dir, Fmt fmt, List<Part> parts) throws IOException {
        for (Part part : parts) {
            Path file = dir.resolve(part.name() + "." + fmt.ext);
            switch (fmt) {
                case CSV -> Files.writeString(file, delimited(part, ','), StandardCharsets.UTF_8);
                case TSV -> Files.writeString(file, delimited(part, '\t'), StandardCharsets.UTF_8);
                case NDJSON -> Files.writeString(file, ndjson(part), StandardCharsets.UTF_8);
                case PARQUET -> writeParquetPart(file, part);
            }
        }
    }

    private static String delimited(Part part, char sep) {
        StringBuilder sb = new StringBuilder(String.join(String.valueOf(sep), part.columns())).append('\n');
        for (List<String> row : part.rows()) {
            sb.append(String.join(String.valueOf(sep), row)).append('\n');
        }
        return sb.toString();
    }

    private static String ndjson(Part part) {
        StringBuilder sb = new StringBuilder();
        for (List<String> row : part.rows()) {
            sb.append('{');
            for (int c = 0; c < part.columns().size(); c++) {
                if (c > 0) {
                    sb.append(',');
                }
                sb.append('"').append(part.columns().get(c)).append("\":");
                if (part.numeric().get(c)) {
                    sb.append(row.get(c));
                } else {
                    sb.append('"').append(row.get(c)).append('"');
                }
            }
            sb.append("}\n");
        }
        return sb.toString();
    }

    /**
     * Parquet divergence is physical: a column is int32, int64 or binary(UTF8) per part, and an absent column is
     * simply not in the part's message type. So {@code widen} is int64 against int32 and {@code conflict} is
     * binary against int32, each irreconcilable or safely widenable in the reader rather than in the text.
     */
    private static void writeParquetPart(Path file, Part part) throws IOException {
        StringBuilder schema = new StringBuilder("message part {");
        for (int c = 0; c < part.columns().size(); c++) {
            String col = part.columns().get(c);
            String type;
            if (part.numeric().get(c) == false) {
                type = "binary " + col + " (UTF8)";
            } else {
                boolean wide = part.rows().isEmpty() == false && isWide(part.rows().get(0).get(c));
                type = (wide ? "int64 " : "int32 ") + col;
            }
            schema.append(" required ").append(type).append(';');
        }
        schema.append(" }");
        List<List<String>> rows = part.rows();
        writeParquet(file, schema.toString(), rows.size(), 1024, (Group g, int i) -> {
            List<String> row = rows.get(i);
            for (int c = 0; c < part.columns().size(); c++) {
                String col = part.columns().get(c);
                if (part.numeric().get(c) == false) {
                    g.add(col, row.get(c));
                } else if (isWide(row.get(c))) {
                    g.add(col, Long.parseLong(row.get(c)));
                } else {
                    g.add(col, Integer.parseInt(row.get(c)));
                }
            }
        });
    }

    private static boolean isWide(String text) {
        try {
            return Long.parseLong(text) > Integer.MAX_VALUE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Declarations
    // ------------------------------------------------------------------------------------------------------

    private static LinkedHashMap<String, DatasetFieldMapping> declaration(Shape shape, Supply supply) {
        LinkedHashMap<String, DatasetFieldMapping> cols = new LinkedHashMap<>();
        if (shape == Shape.DECL_REORDERED) {
            // Same columns and types as the file, declared in a different order than the header.
            cols.put("id", new DatasetFieldMapping("integer", null));
            cols.put("value", new DatasetFieldMapping("integer", null));
            cols.put("order_id", new DatasetFieldMapping("integer", null));
            cols.put("label", new DatasetFieldMapping("keyword", null));
            cols.put("color", new DatasetFieldMapping("integer", null));
        } else {
            cols.put("id", new DatasetFieldMapping("integer", null));
            cols.put("color", new DatasetFieldMapping("integer", null));
            cols.put("label", new DatasetFieldMapping("keyword", null));
            cols.put("order_id", new DatasetFieldMapping("integer", null));
            cols.put("value", new DatasetFieldMapping("integer", null));
        }
        switch (shape) {
            case DECL_ABSENT -> cols.put("missing_col", new DatasetFieldMapping("keyword", null));
            case DECL_NARROWER -> cols.remove("order_id");
            case DECL_CONFLICT -> cols.put("order_id", new DatasetFieldMapping("keyword", null));
            case DECL_WIDEN -> cols.put("order_id", new DatasetFieldMapping("long", null));
            default -> {
                // multi-file shapes declare the canonical five columns; the corpus carries the divergence.
            }
        }
        if (supply == Supply.OVERLAY_RETYPE) {
            cols.put("order_id", new DatasetFieldMapping("keyword", null));
        }
        return cols;
    }

    // ------------------------------------------------------------------------------------------------------
    // One cell
    // ------------------------------------------------------------------------------------------------------

    /** Classification of pass 2 against pass 1 for one aggregate. */
    private record Reading(String outcome, long pass1Rows, long pass2Rows, String note) {}

    /**
     * Every aggregate in a cell gets its OWN corpus directory and its own dataset, so each aggregate's pass 1 is
     * genuinely cold. Sharing one dataset across the six (the ordering the existing fold IT uses) leaves a later
     * aggregate's "cold" pass already served by the records an earlier one harvested, which shows up as
     * {@code VACUOUS_P1_ZERO} and says nothing about that aggregate's own rail. The directory is per aggregate for
     * the same reason it is per cell: {@code DefinitionVersion} folds resource and settings but NOT the mapping,
     * so two datasets over one directory with equal settings share every cache address.
     */
    private void runCell(Cell cell) {
        for (Agg agg : AGGS) {
            String name = "wc" + (cellSeq++);
            try {
                Path dir = newCellDir(name);
                write(dir, cell.fmt(), corpus(cell.shape(), cell.fileCount()));
                register(name, resourceUri(dir, cell.fmt(), cell.fileCount()), cell);
            } catch (Exception e) {
                emit(cell, agg.id(), new Reading("REFUSED_AT_PUT", -1, -1, trim(e)), Map.of());
                continue;
            }
            try {
                measure(cell, name, agg);
            } finally {
                deleteDataset(name);
            }
        }
    }

    /** A fresh, empty directory for one cell, under this method's single temp root. */
    private Path newCellDir(String name) throws IOException {
        if (cellRoot == null) {
            cellRoot = createTempDir();
        }
        Path dir = cellRoot.resolve(name);
        Files.createDirectories(dir);
        return dir;
    }

    private void deleteDataset(String name) {
        try {
            client(internalCluster().getMasterName()).execute(
                DeleteDatasetAction.INSTANCE,
                new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { name })
            ).actionGet(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("cleanup of [{}] failed", name, e);
        }
    }

    /**
     * A one-file cell must address the file DIRECTLY. A glob is multi-file to {@code GlobExpander.isMultiFile}
     * whatever it matches, so a one-file glob routes through {@code resolveMultiFileSource} and never reaches the
     * single-file rails this axis exists to measure.
     */
    private static String resourceUri(Path dir, Fmt fmt, int fileCount) {
        if (fileCount == 1) {
            return StoragePath.fileUri(dir.resolve("part-00." + fmt.ext));
        }
        return StoragePath.fileUri(dir) + "/*." + fmt.ext;
    }

    private void measure(Cell cell, String dataset, Agg agg) {
        String query = "FROM " + dataset + " | STATS " + agg.projection();
        ExternalSourceCacheService cache = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        // Start every reading from an empty coordinator cache. Two things follow: pass 1 is unambiguously cold, and
        // no other cell's records are resident to be evicted under pressure - so a RESCANNED reading is a property
        // of this cell's rail rather than of how many cells ran before it.
        cache.clearAll();
        Map<String, Long> before = counters(cache);
        long pass1;
        String pass1Scan;
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            pass1 = response.documentsFound();
            pass1Scan = scanShape(response);
        } catch (Exception e) {
            emit(cell, agg.id(), new Reading("QUERY_FAILED", -1, -1, trim(e)), Map.of());
            return;
        }
        Map<String, Long> mid = counters(cache);
        long pass2;
        String pass2Scan;
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            pass2 = response.documentsFound();
            pass2Scan = scanShape(response);
        } catch (Exception e) {
            emit(cell, agg.id(), new Reading("WARM_QUERY_FAILED", pass1, -1, trim(e)), Map.of());
            return;
        }
        Map<String, Long> after = counters(cache);

        String outcome;
        String note = "p1=" + pass1Scan + ",p2=" + pass2Scan;
        if (pass1 == 0) {
            // Pass 1 emitted no rows, so a rows-based warm reading is vacuous. This is the normal Parquet case
            // (COUNT(*) folds from footers on the first query) and can also happen when an earlier aggregate in
            // this cell already warmed the records this one needs.
            outcome = "VACUOUS_P1_ZERO";
        } else if (pass2 == 0) {
            outcome = "SERVED";
        } else if (pass2 == pass1) {
            outcome = "RESCANNED";
        } else {
            outcome = "PARTIAL";
        }
        emit(cell, agg.id(), new Reading(outcome, pass1, pass2, note), delta(before, mid, after));
    }

    /**
     * What the scan looked like, from the driver profiles: whether an external scan operator ran at all, how many
     * splits it reported, and — for Parquet, the only reader that publishes them — the per-query footer-cache
     * hit/miss counters. The footer counters have no node-level accessor anywhere (no counter on
     * {@code ParsedFooterCache} or {@code FooterByteCache}, nothing in {@code usageStats()}), so the operator status
     * is the only place they surface; they are read off its rendered XContent because the typed
     * {@code ParquetReaderStatus} lives in a module this source set does not compile against.
     */
    private static String scanShape(EsqlQueryResponse response) {
        List<AsyncExternalSourceOperator.Status> statuses = externalScanStatuses(response);
        if (statuses.isEmpty()) {
            return "no-scan";
        }
        int splitsSum = 0;
        int splitsMax = 0;
        long footerHits = 0;
        long footerMisses = 0;
        for (AsyncExternalSourceOperator.Status status : statuses) {
            splitsSum += status.splitsTotal();
            splitsMax = Math.max(splitsMax, status.splitsTotal());
            String rendered = Strings.toString(status);
            footerHits += firstLong(rendered, FOOTER_HITS);
            footerMisses += firstLong(rendered, FOOTER_MISSES);
        }
        return "scanned,ops="
            + statuses.size()
            + ",splits_sum="
            + splitsSum
            + ",splits_max="
            + splitsMax
            + ",footer="
            + footerHits
            + "/"
            + footerMisses;
    }

    private static final Pattern FOOTER_HITS = Pattern.compile("\"footer_cache_hits\"\\s*:\\s*(\\d+)");
    private static final Pattern FOOTER_MISSES = Pattern.compile("\"footer_cache_misses\"\\s*:\\s*(\\d+)");

    private static long firstLong(String rendered, Pattern pattern) {
        Matcher m = pattern.matcher(rendered);
        long total = 0;
        while (m.find()) {
            total += Long.parseLong(m.group(1));
        }
        return total;
    }

    /** Every numeric entry in the coordinator cache's usage stats, so the Parquet rows can be read off deltas. */
    private static Map<String, Long> counters(ExternalSourceCacheService cache) {
        Map<String, Long> out = new TreeMap<>();
        for (Map.Entry<String, Object> e : cache.usageStats().entrySet()) {
            if (e.getValue() instanceof Number n) {
                out.put(e.getKey(), n.longValue());
            }
        }
        out.put("dataset_aggregate.fallbacks", ExternalSourceCacheTestAccess.datasetAggregateFallbacks(cache));
        return out;
    }

    private static Map<String, String> delta(Map<String, Long> before, Map<String, Long> mid, Map<String, Long> after) {
        Map<String, String> out = new TreeMap<>();
        for (String key : mid.keySet()) {
            long d1 = mid.getOrDefault(key, 0L) - before.getOrDefault(key, 0L);
            long d2 = after.getOrDefault(key, 0L) - mid.getOrDefault(key, 0L);
            if (d1 != 0 || d2 != 0) {
                out.put(key, d1 + "/" + d2);
            }
        }
        return out;
    }

    private void emit(Cell cell, String aggId, Reading reading, Map<String, String> deltas) {
        StringBuilder sb = new StringBuilder(ROW_PREFIX).append(cell.id())
            .append('|')
            .append(aggId)
            .append('|')
            .append(reading.outcome())
            .append('|')
            .append(reading.pass1Rows())
            .append('|')
            .append(reading.pass2Rows())
            .append('|')
            .append(reading.note() == null ? "" : reading.note())
            .append('|');
        boolean first = true;
        for (Map.Entry<String, String> e : deltas.entrySet()) {
            if (first == false) {
                sb.append(' ');
            }
            first = false;
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        logger.info("{}", sb);
    }

    private static String trim(Exception e) {
        String m = e.getMessage();
        if (m == null) {
            m = e.getClass().getSimpleName();
        }
        m = m.replace('|', '/').replace('\n', ' ');
        return m.length() > 220 ? m.substring(0, 220) : m;
    }

    // ------------------------------------------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------------------------------------------

    private final Map<String, Boolean> dataSources = new HashMap<>();

    private void register(String name, String uri, Cell cell) {
        String type = cell.supply() == Supply.DECLARED_ONLY_LOCAL ? "local" : "test";
        String ds = cell.supply() == Supply.DECLARED_ONLY_LOCAL ? "matrix_local_ds" : "matrix_test_ds";
        if (dataSources.containsKey(ds) == false) {
            assertAcked(
                client(internalCluster().getMasterName()).execute(
                    PutDataSourceAction.INSTANCE,
                    new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, ds, type, null, new HashMap<>())
                )
            );
            dataSources.put(ds, true);
        }
        Map<String, Object> settings = new HashMap<>();
        settings.put("format", cell.fmt().setting);
        settings.put("error_mode", cell.errorMode());
        if (cell.resolution() != null) {
            settings.put("schema_resolution", cell.resolution());
            if ("first_file_wins".equals(cell.resolution())) {
                // Accepted only under first_file_wins; it pins part-00 as the anchor.
                settings.put("file_sort_by", "name");
            }
        }
        DatasetMapping mapping = switch (cell.supply()) {
            case INFERRED -> null;
            case DECLARED, DECLARED_ONLY_TEST, DECLARED_ONLY_LOCAL -> new DatasetMapping(
                new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, declaration(cell.shape(), cell.supply()))
            );
            case OVERLAY_IDENTITY, OVERLAY_RETYPE -> new DatasetMapping(
                new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, declaration(cell.shape(), cell.supply()))
            );
        };
        assertAcked(
            client(internalCluster().getMasterName()).execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, ds, uri, null, settings, mapping)
            )
        );
    }

    // ------------------------------------------------------------------------------------------------------
    // Cell set builders
    // ------------------------------------------------------------------------------------------------------

    private static final List<String> RESOLUTIONS = List.of("first_file_wins", "union_by_name", "strict");
    private static final List<String> MODES = List.of("fail_fast", "null_field", "skip_row");

    /** The 14 (supply, resolution) bindings: the supplies that read a resolution times three, plus the two that do not. */
    private static List<Cell> bindings(Fmt fmt, int fileCount, Shape shape, String mode) {
        List<Cell> cells = new ArrayList<>();
        for (Supply supply : List.of(Supply.INFERRED, Supply.DECLARED, Supply.OVERLAY_IDENTITY, Supply.OVERLAY_RETYPE)) {
            for (String resolution : RESOLUTIONS) {
                cells.add(new Cell(fmt, fileCount, shape, supply, resolution, mode));
            }
        }
        cells.add(new Cell(fmt, fileCount, shape, Supply.DECLARED_ONLY_TEST, null, mode));
        cells.add(new Cell(fmt, fileCount, shape, Supply.DECLARED_ONLY_LOCAL, null, mode));
        return cells;
    }

    /** The cheaper binding set used where the full fourteen is not affordable: one resolution per declared supply. */
    private static List<Cell> narrowBindings(Fmt fmt, int fileCount, Shape shape, String mode) {
        List<Cell> cells = new ArrayList<>();
        for (String resolution : RESOLUTIONS) {
            cells.add(new Cell(fmt, fileCount, shape, Supply.INFERRED, resolution, mode));
        }
        cells.add(new Cell(fmt, fileCount, shape, Supply.DECLARED, "first_file_wins", mode));
        cells.add(new Cell(fmt, fileCount, shape, Supply.OVERLAY_IDENTITY, "first_file_wins", mode));
        cells.add(new Cell(fmt, fileCount, shape, Supply.OVERLAY_RETYPE, "first_file_wins", mode));
        cells.add(new Cell(fmt, fileCount, shape, Supply.DECLARED_ONLY_TEST, null, mode));
        cells.add(new Cell(fmt, fileCount, shape, Supply.DECLARED_ONLY_LOCAL, null, mode));
        return cells;
    }

    private static final List<Shape> MULTI_SHAPES = List.of(
        Shape.HOMOGENEOUS,
        Shape.MISMATCHED,
        Shape.MISALIGNED,
        Shape.ABSENT,
        Shape.CONFLICT,
        Shape.WIDEN
    );

    private static final List<Shape> SINGLE_SHAPES = List.of(
        Shape.EXACT,
        Shape.DECL_ABSENT,
        Shape.DECL_NARROWER,
        Shape.DECL_CONFLICT,
        Shape.DECL_WIDEN,
        Shape.DECL_REORDERED
    );

    private void runAll(List<Cell> cells) {
        logger.info("{}CELLS|-|{}|-|-|-|", ROW_PREFIX, cells.size());
        for (Cell cell : cells) {
            runCell(cell);
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Controls. A table where everything warms, or nothing does, must fail here rather than read as a finding.
    // ------------------------------------------------------------------------------------------------------

    public void testControls() throws Exception {
        // Positive, text: the cell ExternalMultiFileWarmAggregateFoldIT already asserts.
        Path dir = createTempDir().resolve("ctl_pos");
        Files.createDirectories(dir);
        write(dir, Fmt.CSV, corpus(Shape.HOMOGENEOUS, MULTI_FILE_COUNT));
        String uri = StoragePath.fileUri(dir) + "/*.csv";
        Cell positive = new Cell(Fmt.CSV, MULTI_FILE_COUNT, Shape.HOMOGENEOUS, Supply.INFERRED, "first_file_wins", "fail_fast");
        register("ctl_pos", uri, positive);
        String query = "FROM ctl_pos | STATS c = COUNT(*)";
        long cold;
        try (var r = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            cold = r.documentsFound();
        }
        assertTrue("positive control: pass 1 must read rows, got " + cold, cold > 0);
        try (var r = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            assertEquals("POSITIVE CONTROL FAILED: an unchanged homogeneous inferred CSV corpus must warm", 0L, r.documentsFound());
        }
        logger.info("{}CONTROL|positive_text|PASS|{}|0||", ROW_PREFIX, cold);

        // Negative, stage 1: evict every PER-FILE record and the listing. This does NOT reach the memoized dataset
        // aggregate, which lives in its own store, so a COUNT(*) can still be served off the fallback rail. Recorded
        // rather than asserted: which of the two answers appears is itself the measurement.
        ExternalSourceCacheService cache = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        int evicted = ExternalSourceCacheTestAccess.invalidatePerFileSchemaEntries(cache, dir.getFileName().toString());
        ExternalSourceCacheTestAccess.invalidateListings(cache);
        assertTrue("negative control could not evict anything, so it proves nothing", evicted > 0);
        long fallbacksBefore = ExternalSourceCacheTestAccess.datasetAggregateFallbacks(cache);
        long afterPerFileEviction;
        try (var r = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            afterPerFileEviction = r.documentsFound();
        }
        logger.info(
            "{}CONTROL|negative_per_file_evicted|INFO|{}|{}|aggregate_fallback_delta={}|",
            ROW_PREFIX,
            cold,
            afterPerFileEviction,
            ExternalSourceCacheTestAccess.datasetAggregateFallbacks(cache) - fallbacksBefore
        );

        // Negative, stage 2: the whole coordinator cache. Nothing survives, so the query MUST re-scan. If this does
        // not go red the instrument cannot tell a served cell from a re-scanned one and the whole table is void.
        cache.clearAll();
        try (var r = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(2))) {
            assertEquals(
                "NEGATIVE CONTROL FAILED: with the whole coordinator cache cleared the query must re-scan",
                cold,
                r.documentsFound()
            );
        }
        logger.info("{}CONTROL|negative_cache_cleared|PASS|{}|{}||", ROW_PREFIX, cold, cold);

        // Positive, single-file declared: rail C. If this is cold the single-file/multi-file asymmetry cells are moot.
        Path single = createTempDir().resolve("ctl_single");
        Files.createDirectories(single);
        write(single, Fmt.CSV, corpus(Shape.EXACT, 1));
        Cell singleDeclared = new Cell(Fmt.CSV, 1, Shape.EXACT, Supply.DECLARED, "first_file_wins", "fail_fast");
        register("ctl_single", resourceUri(single, Fmt.CSV, 1), singleDeclared);
        String singleQuery = "FROM ctl_single | STATS c = COUNT(*)";
        long singleCold;
        try (var r = run(syncEsqlQueryRequest(singleQuery).profile(true), TimeValue.timeValueMinutes(2))) {
            singleCold = r.documentsFound();
        }
        long singleWarm;
        try (var r = run(syncEsqlQueryRequest(singleQuery).profile(true), TimeValue.timeValueMinutes(2))) {
            singleWarm = r.documentsFound();
        }
        logger.info("{}CONTROL|positive_single_declared|{}|{}|{}||", ROW_PREFIX, singleWarm == 0 ? "PASS" : "COLD", singleCold, singleWarm);

        // Positive, Parquet: rows say nothing, so the reading is the cache-counter delta.
        Path pq = createTempDir().resolve("ctl_pq");
        Files.createDirectories(pq);
        write(pq, Fmt.PARQUET, corpus(Shape.HOMOGENEOUS, 1));
        Cell pqCell = new Cell(Fmt.PARQUET, 1, Shape.EXACT, Supply.INFERRED, "first_file_wins", "fail_fast");
        register("ctl_pq", resourceUri(pq, Fmt.PARQUET, 1), pqCell);
        String pqQuery = "FROM ctl_pq | STATS c = COUNT(*)";
        Map<String, Long> b0 = counters(cache);
        try (var r = run(syncEsqlQueryRequest(pqQuery).profile(true), TimeValue.timeValueMinutes(2))) {
            logger.info("{}CONTROL|parquet_pass1_rows|INFO|{}|-||", ROW_PREFIX, r.documentsFound());
        }
        Map<String, Long> b1 = counters(cache);
        try (var r = run(syncEsqlQueryRequest(pqQuery).profile(true), TimeValue.timeValueMinutes(2))) {
            logger.info("{}CONTROL|parquet_pass2_rows|INFO|-|{}||", ROW_PREFIX, r.documentsFound());
        }
        Map<String, Long> b2 = counters(cache);
        emit(pqCell, "control_parquet", new Reading("COUNTERS", -1, -1, "cold/warm deltas"), delta(b0, b1, b2));
    }

    // ------------------------------------------------------------------------------------------------------
    // Groups. One method per (format, arity) so a group can be run on its own; the cross inside it is the full
    // product of shape x (supply, schema_resolution) x error_mode.
    // ------------------------------------------------------------------------------------------------------

    /** Every multi-file cell for one format: 6 shapes x 14 bindings x 3 error modes. */
    private List<Cell> multiFileCross(Fmt fmt) {
        List<Cell> cells = new ArrayList<>();
        for (String mode : MODES) {
            for (Shape shape : MULTI_SHAPES) {
                cells.addAll(bindings(fmt, MULTI_FILE_COUNT, shape, mode));
            }
        }
        return cells;
    }

    /**
     * Every single-file cell for one format. With one file there is no file-against-file disagreement, so the shape
     * axis becomes declaration-against-file and applies only to the supplies that carry a declaration; the inferred
     * single-file read has exactly one shape, the file itself.
     */
    private List<Cell> singleFileCross(Fmt fmt) {
        List<Cell> cells = new ArrayList<>();
        for (String mode : MODES) {
            for (String resolution : RESOLUTIONS) {
                cells.add(new Cell(fmt, 1, Shape.EXACT, Supply.INFERRED, resolution, mode));
            }
            for (Shape shape : SINGLE_SHAPES) {
                for (Supply supply : List.of(Supply.DECLARED, Supply.OVERLAY_IDENTITY, Supply.OVERLAY_RETYPE)) {
                    for (String resolution : RESOLUTIONS) {
                        cells.add(new Cell(fmt, 1, shape, supply, resolution, mode));
                    }
                }
                cells.add(new Cell(fmt, 1, shape, Supply.DECLARED_ONLY_TEST, null, mode));
                cells.add(new Cell(fmt, 1, shape, Supply.DECLARED_ONLY_LOCAL, null, mode));
            }
        }
        return cells;
    }

    public void testCsvMultiFile() {
        runAll(multiFileCross(Fmt.CSV));
    }

    public void testCsvSingleFile() {
        runAll(singleFileCross(Fmt.CSV));
    }

    public void testTsvMultiFile() {
        runAll(multiFileCross(Fmt.TSV));
    }

    public void testTsvSingleFile() {
        runAll(singleFileCross(Fmt.TSV));
    }

    public void testNdjsonMultiFile() {
        runAll(multiFileCross(Fmt.NDJSON));
    }

    public void testNdjsonSingleFile() {
        runAll(singleFileCross(Fmt.NDJSON));
    }

    /**
     * Parquet multi-file. Its warm signal is not rows: {@code PushStatsToExternalSource} folds COUNT(*) from footers
     * on the FIRST query, so pass 1 is already zero and the cell reads {@code VACUOUS_P1_ZERO}. The reading for
     * Parquet is the cache-counter delta column, not the outcome column.
     */
    public void testParquetMultiFile() {
        runAll(multiFileCross(Fmt.PARQUET));
    }

    public void testParquetSingleFile() {
        runAll(singleFileCross(Fmt.PARQUET));
    }
}
