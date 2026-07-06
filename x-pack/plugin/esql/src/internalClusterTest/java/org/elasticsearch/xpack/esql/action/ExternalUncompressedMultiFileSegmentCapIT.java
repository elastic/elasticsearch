/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end regression test for ES|QL aggregations over an uncompressed multi-file CSV/NDJSON
 * glob dataset. Such reads route through {@code SEGMENTABLE_UNCOMPRESSED} → {@code ParallelParsingCoordinator} →
 * {@code AsReadyParallelIterator}, which dispatches byte-range segments in a sliding window bounded by the
 * {@code max_concurrent_open_segments} pragma and emits their pages as they complete.
 * <p>
 * To actually exercise that path (rather than the single-threaded fallback), each file must be split into
 * several intra-file segments: {@code ParallelParsingCoordinator.parallelRead} only segments when the read
 * length is at least {@code 2 × minimumSegmentSize}. CSV's floor is a fixed 1 MiB, so its files are sized a
 * few MiB; NDJSON exposes a per-query {@code segment_size}, so small files plus a small {@code segment_size}
 * suffice. We also keep {@code target_split_size} large so each file is one macro-split fed whole to
 * {@code parallelRead} (a small {@code target_split_size} would chop files below the segmenting floor and
 * silently route everything through the fallback). With {@code max_concurrent_open_segments} set below the
 * per-file segment count, the window genuinely binds.
 * <p>
 * Column {@code a} is globally unique per row, so {@code COUNT/MIN/MAX} together prove the windowed dispatch
 * dropped no segment, double-counted none, and preserved row accounting across segment boundaries — the
 * end-to-end counterpart to the unit-level bound in
 * {@code ParallelParsingCoordinatorTests#testCapBoundsConcurrentOpenSegments}.
 */
public class ExternalUncompressedMultiFileSegmentCapIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 3;
    // CSV cannot lower its 1 MiB segment floor, so files must clear 2 MiB and carry several segments.
    private static final int CSV_FILE_BYTES = 3_500_000;
    // NDJSON sets segment_size=64kb below, so small files already split into many segments.
    private static final int NDJSON_FILE_BYTES = 512_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 selects the SEGMENTABLE_UNCOMPRESSED parallel-parse path; the cap of 2,
        // below the per-file segment count, makes the sliding window actually bind.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 8).put("max_concurrent_open_segments", 2).build());
    }

    public void testCsvMultiFileGlobAggregatesAllRows() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeCsvFile(dir.resolve("part-" + f + ".csv"), total);
        }
        // target_split_size large => one macro-split per file, so parallelRead segments the whole file.
        assertGlobAggregates(globUri(dir, "*.csv"), Map.of("target_split_size", "256mb"), total);
    }

    public void testTsvMultiFileGlobAggregatesAllRows() throws Exception {
        // Same reader as CSV, but CSV/TSV have a history of failing differently (e.g. the TSV record-boundary
        // scanner), so exercise it as its own arm rather than assuming CSV coverage carries over.
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeTsvFile(dir.resolve("part-" + f + ".tsv"), total);
        }
        assertGlobAggregates(globUri(dir, "*.tsv"), Map.of("target_split_size", "256mb"), total);
    }

    public void testNdjsonMultiFileGlobAggregatesAllRows() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        // segment_size small => each small file still splits into many segments; target_split_size large.
        assertGlobAggregates(globUri(dir, "*.ndjson"), Map.of("segment_size", "64kb", "target_split_size", "256mb"), total);
    }

    /**
     * Runs {@code STATS COUNT/MIN/MAX} over the glob and asserts every row from every file is seen exactly
     * once (column {@code a} runs 0..total-1 globally), then confirms the async source operator emitted them
     * all — i.e. the windowed dispatch lost nothing.
     */
    private void assertGlobAggregates(String globUri, Map<String, Object> settings, long total) throws Exception {
        String dataset = registerDataset("segment_cap", globUri, settings);
        String query = "FROM " + dataset + " | STATS c = COUNT(*), mn = MIN(a), mx = MAX(a)";

        var request = syncEsqlQueryRequest(query);
        request.profile(true);

        try (var response = run(request, TimeValue.timeValueMinutes(5))) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.size(), equalTo(3));
            assertThat(columns.get(0).name(), equalTo("c"));
            assertThat(columns.get(1).name(), equalTo("mn"));
            assertThat(columns.get(2).name(), equalTo("mx"));

            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), equalTo(1));
            List<Object> row = values.get(0);
            assertThat("every row from every file must be counted exactly once", ((Number) row.get(0)).longValue(), equalTo(total));
            assertThat(((Number) row.get(1)).longValue(), equalTo(0L));
            assertThat(((Number) row.get(2)).longValue(), equalTo(total - 1));

            // Confirm the read went through the async external source operator and emitted every row
            // (no segment silently dropped by the windowed dispatch).
            long rowsEmitted = 0;
            boolean foundAsyncOperator = false;
            for (var driver : response.profile().drivers()) {
                for (var op : driver.operators()) {
                    if (op.status() instanceof AsyncExternalSourceOperator.Status status) {
                        foundAsyncOperator = true;
                        rowsEmitted += status.rowsEmitted();
                    }
                }
            }
            assertTrue("expected at least one AsyncExternalSourceOperator in the profile", foundAsyncOperator);
            assertThat("source operator must emit every row across all files", rowsEmitted, equalTo(total));
        }
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    /** Writes a CSV file of at least {@link #CSV_FILE_BYTES}, {@code a} starting at {@code base}; returns rows written. */
    private static long writeCsvFile(Path file, long base) throws Exception {
        return writeDelimited(file, base, ',');
    }

    /** Writes a TSV file of at least {@link #CSV_FILE_BYTES} (tab-delimited), {@code a} starting at {@code base}; returns rows written. */
    private static long writeTsvFile(Path file, long base) throws Exception {
        return writeDelimited(file, base, '\t');
    }

    private static long writeDelimited(Path file, long base, char delimiter) throws Exception {
        long rows = 0;
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            String header = "a" + delimiter + "b\n";
            w.write(header);
            int written = header.length();
            while (written < CSV_FILE_BYTES) {
                long a = base + rows;
                String line = a + Character.toString(delimiter) + (a * 10) + "\n";
                w.write(line);
                written += line.length();
                rows++;
            }
        }
        return rows;
    }

    /** Writes an NDJSON file of at least {@link #NDJSON_FILE_BYTES}, {@code a} starting at {@code base}; returns rows written. */
    private static long writeNdjsonFile(Path file, long base) throws Exception {
        long rows = 0;
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            int written = 0;
            while (written < NDJSON_FILE_BYTES) {
                long a = base + rows;
                String line = "{\"a\":" + a + ",\"b\":" + (a * 10) + "}\n";
                w.write(line);
                written += line.length();
                rows++;
            }
        }
        return rows;
    }
}
