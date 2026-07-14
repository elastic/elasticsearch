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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Isolation test for the SECOND, TTL-independent cause of the ndjson warm-{@code COUNT(*)} loss observed at
 * ClickBench-100M scale: a compressed (streaming-coordinator) multi-file source whose per-file schema-cache
 * entries carry MANY per-stripe sub-entries. Each committed file entry retains one nested map per stripe
 * ({@code _stats.stripe.<k>}) FOREVER — even after the whole-file {@code 0..K} fold has materialized the
 * authoritative {@code _stats.row_count} — so a many-stripe file's entry weight is O(stripe count), not O(1).
 * With enough files × stripes the schema cache's weight budget is exceeded and the LRU evicts already-committed
 * entries; the multi-file warm serve is all-or-nothing ({@code aggregateFileStatistics} returns null if ANY file
 * lacks stats), so warm {@code COUNT(*)} re-scans the whole source. NDJSON hits this first because its
 * decompressed footprint (repeated JSON keys) is 2–3× CSV/TSV at equal rows, so it packs 2–3× the stripes per
 * file; TSV over the same shape stays under budget and short-circuits — the bench discriminator.
 * Post-{@code clearStripeState} (#153085) both formats compact to O(1) and short-circuit identically;
 * the TSV variant remains as a format-parity gate.
 * <p>
 * This test forces the condition at IT scale by pinning a small schema cache (40 KB, ~8 KB schema budget)
 * and a 64 KB stripe grid. At ~50 B/row (NDJSON) each 8,000-row file is ~400 KB decompressed, yielding
 * ~6 stripes per file — enough to overflow the 8 KB schema budget across 4 files before
 * {@code clearStripeState} compacts them. The gzip-tsv control drives the SAME
 * {@code StreamingParallelParsingCoordinator} through {@code CsvFormatReader}.
 */
public class ExternalCompressedMultiFileCountWarmFoldIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 4;
    private static final int ROWS_PER_FILE = 8_000;
    private static final long TOTAL = (long) FILE_COUNT * ROWS_PER_FILE;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, GzipDataSourcePlugin.class);
    }

    /**
     * Registers {@link InternalExchangePlugin} so the short inactive-sink reap interval below is a known
     * node setting. Without it the reaper runs at its 5-minute default and a data-node exchange sink whose
     * async post-query cleanup lags the test's teardown window is not removed in time — the CI-only
     * "Leftover exchanges" / "Request breaker not reset" teardown failure this test was hitting.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalExchangePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Stripe grid at its 64kb floor: the smallest addressable grid, so a few hundred KB per file yields
            // several per-stripe sub-entries without a heavy scan. Row VOLUME is irrelevant to the eviction — the
            // retained per-stripe entry WEIGHT is what overflows the cache; a few stripes over a tiny budget suffice.
            .put("esql.source.cache.stripe.size", "64kb")
            // Deliberately tiny schema cache: the whole-file base entries fit, but the retained per-stripe
            // sub-entries push the multi-file working set over the (20%-of-this) schema budget, so committed
            // entries are evicted before the warm serve can fold across all files. clearStripeState is what
            // brings the folded entries back to O(1) so they survive. Method execution order
            // is irrelevant: clearStripeState compacts each file's entry after its fold, so
            // leftover state from one method cannot exhaust the budget for the next.
            .put("esql.source.cache.size", "40kb")
            // Reap inactive exchange sinks within a few seconds (default is 5 minutes) so a data-node sink whose
            // async cleanup trails the query response is released well inside the exchange/breaker teardown checks.
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testGzipNdjsonManyStripeMultiFileCountShortCircuits() throws Exception {
        assertWarmCountShortCircuits(writeAndRegister("ndjson.gz"));
    }

    public void testGzipTsvManyStripeMultiFileCountShortCircuits() throws Exception {
        assertWarmCountShortCircuits(writeAndRegister("tsv.gz"));
    }

    private void assertWarmCountShortCircuits(String dataset) {
        String countQuery = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = runProfiled(countQuery)) {
            assertSingleLong(response, TOTAL);
            assertThat("cold COUNT(*) reads every row", response.documentsFound(), equalTo(TOTAL));
        }
        try (var response = runProfiled(countQuery)) {
            assertSingleLong(response, TOTAL);
            assertThat(
                "warm COUNT(*) must short-circuit across a many-stripe compressed multi-file source",
                response.documentsFound(),
                equalTo(0L)
            );
        }
    }

    /**
     * Runs {@code query} with profiling on. This test's weight-driven eviction is independent of
     * {@code parsing_parallelism} (stripe count comes from the stripe grid, not the parse degree), so nothing
     * is pinned — and pinning would be illegal anyway: {@code parsing_parallelism} is a snapshot-only pragma
     * and a request carrying it is rejected in release builds.
     */
    private EsqlQueryResponse runProfiled(String query) {
        return run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5));
    }

    private static void assertSingleLong(EsqlQueryResponse response, long expected) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    private String writeAndRegister(String format) throws IOException {
        Path dir = createTempDir();
        long v = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            Path file = dir.resolve("part-" + f + "." + format);
            v = switch (format) {
                case "ndjson.gz" -> writeGzipNdjson(file, v);
                case "tsv.gz" -> writeGzipTsv(file, v);
                default -> throw new IllegalArgumentException("unknown format: " + format);
            };
        }
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return registerDataset("compressed_multifile_" + format.replace('.', '_'), dirUri + "*." + format, Map.of());
    }

    private static final String PAD = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    private static long writeGzipNdjson(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder();
        long v = base;
        for (int i = 0; i < ROWS_PER_FILE; i++, v++) {
            sb.append("{\"id\":").append(v).append(",\"value\":").append(v).append(",\"pad\":\"").append(PAD).append("\"}\n");
        }
        writeGzipped(file, sb.toString());
        return v;
    }

    private static long writeGzipTsv(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder("id\tvalue\tpad\n");
        long v = base;
        for (int i = 0; i < ROWS_PER_FILE; i++, v++) {
            sb.append(v).append('\t').append(v).append('\t').append(PAD).append('\n');
        }
        writeGzipped(file, sb.toString());
        return v;
    }
}
