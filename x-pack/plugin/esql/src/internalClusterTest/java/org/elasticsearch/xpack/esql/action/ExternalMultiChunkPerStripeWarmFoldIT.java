/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Warm short-circuit regression coverage for the <b>parallel multiple-chunks-per-stripe</b> read geometry, for
 * EVERY row format. The sibling fold ITs leave a hole this test closes:
 * <ul>
 *   <li>{@link ExternalMultiFileWarmAggregateFoldIT} writes files below the segment-size chunking threshold, so
 *       each file is read whole (one authoritative summary) and never parallel-chunked.</li>
 *   <li>{@link ExternalNdJsonMultiStripeFoldIT} pins parallelism to 1 (again a whole-file read), and uses a 64kb
 *       stripe far smaller than a read chunk, so a chunk spans MANY stripes.</li>
 * </ul>
 * Neither exercises the bench shape: a file large enough to split into several real read chunks, with a stripe
 * grid LARGER than a chunk so each stripe is stitched together from MULTIPLE parallel chunks via the coordinator's
 * per-stripe byte-range interval-cover fold. That is exactly the geometry where NDJSON's retired page-capped
 * striping used to leave the fold incomplete and warm {@code COUNT(*)}/{@code MIN}/{@code MAX} re-scanned the file
 * while CSV/TSV short-circuited. Any reader that stops committing per-stripe stats for this realistic shape (or
 * regresses the whole-file EOF marker so the {@code 0..K} fold never closes) must fail this test.
 * <p>
 * Each file is ~12 MB (well over the default 4 MB segment size, so parallelism 4 yields ~3 chunks per file), the
 * stripe grid is pinned to 6 MB (larger than a 4 MB chunk, so every stripe spans a chunk boundary), and the
 * dataset spans multiple files. Columns cover a LONG stats column and a temporal (DATETIME) stats column — the
 * bench {@code EventDate} shape — so both {@code MIN}/{@code MAX} over a plain integer and over a date short-circuit.
 * Runs for CSV, NDJSON, and gzip-compressed NDJSON (which drives the sibling {@code StreamingParallelParsingCoordinator}
 * over the decompressed stream); TSV shares the CSV reader's stripe path.
 */
public class ExternalMultiChunkPerStripeWarmFoldIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 2;
    private static final int ROWS_PER_FILE = 120_000; // ~12 MB/file once padded -> 3 chunks at the 4 MB segment size
    private static final long TOTAL = (long) FILE_COUNT * ROWS_PER_FILE;
    private static final long TS_BASE_MILLIS = 1_577_836_800_000L; // 2020-01-01T00:00:00Z

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, GzipDataSourcePlugin.class);
    }

    /**
     * Registers {@link InternalExchangePlugin} so the short inactive-sink reap interval below is a known
     * node setting. Without it the reaper runs at its 5-minute default and a data-node exchange sink whose
     * async post-query cleanup lags the test's teardown window is not removed in time — the CI-only
     * "Leftover exchanges" / "Request breaker not reset" teardown failure the sibling multi-file fold IT hit.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalExchangePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Stripe (6 MB) LARGER than the default 4 MB read segment: every stripe spans a chunk boundary, so
            // the per-stripe interval-cover must stitch fragments from more than one parallel chunk. This is the
            // "multiple chunks per stripe" geometry the sibling fold ITs (64kb stripe, or parallelism 1) never hit.
            .put("esql.source.cache.stripe.size", "6mb")
            // Reap inactive exchange sinks within a few seconds (default is 5 minutes) so a data-node sink whose
            // async cleanup trails the query response is released well inside the exchange/breaker teardown checks.
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 selects the parallel-parse path so each ~12 MB file is read in several 4 MB
        // chunks, the production shape at the 1rg-per-file ClickBench layout.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 4).build());
    }

    /**
     * Pins every query to one coordinator: the reconciled schema cache is per-coordinator, so the cold scan and
     * the warm short-circuit must hit the same node (mirrors {@link ExternalMultiFileWarmAggregateFoldIT}).
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testCsvMultiChunkPerStripeWarmShortCircuit() throws Exception {
        assertWarmAggregatesShortCircuit(writeAndRegister("csv"));
    }

    public void testNdjsonMultiChunkPerStripeWarmShortCircuit() throws Exception {
        assertWarmAggregatesShortCircuit(writeAndRegister("ndjson"));
    }

    /**
     * gzip-compressed NDJSON drives {@code StreamingParallelParsingCoordinator} (the decompressed-stream parallel
     * path) instead of {@code ParallelParsingCoordinator}. It is a stream-only source, so it is read as a single
     * serial decompressing stream cut into record-aligned chunks; the per-stripe fold must still reach whole-file
     * completeness over the decompressed byte grid for the warm aggregate to short-circuit.
     */
    public void testGzipNdjsonMultiChunkPerStripeWarmShortCircuit() throws Exception {
        assertWarmAggregatesShortCircuit(writeAndRegister("ndjson.gz"));
    }

    /**
     * Cold-then-warm for COUNT(*), MIN/MAX over a LONG column, and MIN/MAX over a DATETIME column. Each warm pass
     * must short-circuit ({@code documentsFound == 0}), proving the per-stripe fold reached whole-file completeness
     * across chunks and files. COUNT(*) runs first so its row-count-only per-file entries are in place before
     * MIN/MAX, reproducing the production ordering.
     */
    private void assertWarmAggregatesShortCircuit(String dataset) {
        String countQuery = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, TOTAL);
            assertThat("cold COUNT(*) reads every row", response.documentsFound(), equalTo(TOTAL));
        }
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, TOTAL);
            assertThat("warm COUNT(*) must short-circuit across multi-chunk-per-stripe files", response.documentsFound(), equalTo(0L));
        }

        String longQuery = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(longQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, TOTAL - 1);
            assertThat("cold MIN/MAX(long) reads every row", response.documentsFound(), equalTo(TOTAL));
        }
        try (var response = run(syncEsqlQueryRequest(longQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, TOTAL - 1);
            assertThat("warm MIN/MAX(long) must short-circuit across multi-chunk-per-stripe files", response.documentsFound(), equalTo(0L));
        }

        // Temporal (DATETIME) stats column — the bench EventDate shape. MIN/MAX over a date must short-circuit AND
        // serve the true extrema. The cold full scan is authoritative; the warm serve must return the identical row
        // (representation-agnostic — the DATETIME rendering is whatever the cold scan produced), not just fire.
        String dateQuery = "FROM " + dataset + " | STATS lo = MIN(ts), hi = MAX(ts)";
        List<Object> coldDateExtrema;
        try (var response = run(syncEsqlQueryRequest(dateQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            coldDateExtrema = getValuesList(response).get(0);
            assertThat("cold MIN/MAX(date) reads every row", response.documentsFound(), equalTo(TOTAL));
        }
        try (var response = run(syncEsqlQueryRequest(dateQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertThat("warm MIN/MAX(date) must serve the cold extrema", getValuesList(response).get(0), equalTo(coldDateExtrema));
            assertThat("warm MIN/MAX(date) must short-circuit across multi-chunk-per-stripe files", response.documentsFound(), equalTo(0L));
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

    /** Writes {@code FILE_COUNT} files of the given format into a directory and registers the glob as a dataset. */
    private String writeAndRegister(String format) throws IOException {
        Path dir = createTempDir();
        long v = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            Path file = dir.resolve("part-" + f + "." + format);
            v = switch (format) {
                case "csv" -> writeCsvFile(file, v);
                case "ndjson" -> writeNdjsonFile(file, v, false);
                case "ndjson.gz" -> writeNdjsonFile(file, v, true);
                default -> throw new IllegalArgumentException("unknown format: " + format);
            };
        }
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return registerDataset("multichunk_" + format, dirUri + "*." + format, Map.of());
    }

    // ~50 bytes of padding per row so a 120k-row file clears the 4 MB segment*2 chunking threshold with margin.
    private static final String PAD = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    /** Writes a CSV file; {@code value} runs from {@code base}. Returns the next global value. */
    private static long writeCsvFile(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder("id,value,ts,pad\n");
        long v = base;
        for (int i = 0; i < ROWS_PER_FILE; i++, v++) {
            sb.append(v)
                .append(',')
                .append(v)
                .append(',')
                .append(Instant.ofEpochMilli(TS_BASE_MILLIS + v * 1000L))
                .append(',')
                .append(PAD)
                .append('\n');
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        return v;
    }

    /**
     * Writes an NDJSON file (optionally gzip-compressed); {@code value} runs from {@code base}. Returns the next
     * global value. When {@code gzip} the file exceeds the chunk size in DECOMPRESSED bytes, so the streaming
     * coordinator still cuts several chunks over the inflated stream.
     */
    private static long writeNdjsonFile(Path file, long base, boolean gzip) throws IOException {
        StringBuilder sb = new StringBuilder();
        long v = base;
        for (int i = 0; i < ROWS_PER_FILE; i++, v++) {
            sb.append("{\"id\":")
                .append(v)
                .append(",\"value\":")
                .append(v)
                .append(",\"ts\":\"")
                .append(Instant.ofEpochMilli(TS_BASE_MILLIS + v * 1000L))
                .append("\",\"pad\":\"")
                .append(PAD)
                .append("\"}\n");
        }
        if (gzip) {
            writeGzipped(file, sb.toString());
        } else {
            Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        }
        return v;
    }
}
