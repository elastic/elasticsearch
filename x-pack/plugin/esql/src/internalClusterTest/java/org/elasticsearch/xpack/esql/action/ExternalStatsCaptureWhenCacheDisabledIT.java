/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matcher;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * A disabled external-source cache must leave no statistics machinery on the read path.
 *
 * <p>Statistics are harvested to populate that cache: with it disabled,
 * {@code ExternalSourceCacheService.reconcileSourceStatsFromContributions} returns on its first line, so every
 * contribution a scan produces is discarded on the coordinator.
 *
 * <p>Two terms are asserted separately because they scale differently. <b>Measurements</b> — row counts and
 * per-column min/max/null — scale with rows scanned. The <b>unclean-close marker</b> is one map per FILE, written
 * by whichever parsing coordinator ran when a read did not drain cleanly, so it scales with file count and
 * survives any fix scoped to the measurements.
 *
 * <p>Reaching that marker takes care. A {@code LIMIT} pushed INTO the source does not reach it: a pushed limit
 * leaves {@code rowLimit != NO_LIMIT}, and {@code AsyncExternalSourceOperatorFactory.openWithParallelism} returns
 * null for that, so no coordinator is built and the single-threaded read writes no marker. It needs a read the
 * source runs uncapped that still stops short — hence the {@code EVAL}-derived predicate below, and the pinned
 * parsing parallelism, since a coordinator is only built above one.
 *
 * <p>Each arm reads its own directory and dataset, so no scan can answer from an earlier one's statistics.
 */
public class ExternalStatsCaptureWhenCacheDisabledIT extends AbstractExternalDataSourceIT {

    private static final long TS_BASE_MILLIS = 1_577_836_800_000L; // 2020-01-01T00:00:00Z

    /** Large enough that a filtered LIMIT a few rows deep stops the read well before EOF. */
    private static final int ROWS = 80_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, GzipDataSourcePlugin.class);
    }

    /**
     * A parsing coordinator is built only above parallelism 1, and the marker is written by a coordinator, so the
     * early-close arms pin it rather than depending on the runner's processor count. A release build rejects
     * pragmas unless the request opts in, which {@link #run} does.
     */
    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("external_parsing_parallelism", 4).build());
    }

    /**
     * Pin every query to one coordinator, as the sibling warm-fold ITs do: the enabled-cache control must be a
     * genuine COLD scan, and what has already been harvested is held per coordinator.
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        request.pragmas(getPragmas());
        request.acceptedPragmaRisks(true);
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testCsvCleanScanHarvestsNothingWithTheCacheDisabled() throws Exception {
        assertCleanScanHarvestOnlyWhenEnabled("csv");
    }

    public void testNdjsonCleanScanHarvestsNothingWithTheCacheDisabled() throws Exception {
        assertCleanScanHarvestOnlyWhenEnabled("ndjson");
    }

    /**
     * The measurement term. Row count is recorded for every scope but {@code none}, so an exhaustive
     * {@code COUNT(*)} harvests without depending on which columns the query projects.
     */
    private void assertCleanScanHarvestOnlyWhenEnabled(String format) throws Exception {
        try {
            setCacheEnabled(true);
            Map<String, List<Map<String, Object>>> enabled = scan(format, "clean_on", "| STATS COUNT(*)", exhausted());
            assertThat("control: an enabled cache must harvest something, or this test proves nothing", enabled, not(anEmptyMap()));

            setCacheEnabled(false);
            Map<String, List<Map<String, Object>>> disabled = scan(format, "clean_off", "| STATS COUNT(*)", exhausted());
            assertThat("a disabled cache must harvest nothing", disabled, anEmptyMap());

            // Back on, against a file neither earlier arm read: the flag is resolved per read, not captured once.
            setCacheEnabled(true);
            Map<String, List<Map<String, Object>>> reenabled = scan(format, "clean_again", "| STATS COUNT(*)", exhausted());
            assertThat("re-enabling the cache must put harvesting back", reenabled, not(anEmptyMap()));
        } finally {
            restoreCacheSetting();
        }
    }

    /** The per-file term on the segmented coordinator, which plain uncompressed CSV resolves to. */
    public void testCsvEarlyCloseWritesNoPerFileMarkerWithTheCacheDisabled() throws Exception {
        assertEarlyCloseMarkerOnlyWhenEnabled("csv");
    }

    /** The same on the streaming coordinator, which a compressed object takes because it cannot be range-split. */
    public void testGzippedCsvEarlyCloseWritesNoPerFileMarkerWithTheCacheDisabled() throws Exception {
        assertEarlyCloseMarkerOnlyWhenEnabled("csv.gz");
    }

    /**
     * The per-file term. The filter is computed by {@code EVAL}, so the planner cannot push it and cannot turn the
     * {@code LIMIT} into a source row cap; the source runs uncapped and stops once the downstream limiter is
     * satisfied, which is the non-clean close a coordinator answers with its marker.
     */
    private void assertEarlyCloseMarkerOnlyWhenEnabled(String format) throws Exception {
        String tail = "| EVAL doubled = value * 2 | WHERE doubled > 1 | LIMIT 5";
        try {
            setCacheEnabled(true);
            Map<String, List<Map<String, Object>>> enabled = scan(format, "limit_on", tail, stoppedShort());
            // Not merely non-empty: a non-empty map could hold a completed stripe rather than the marker.
            assertThat(
                "control: an early close against an enabled cache must write the per-file marker itself; got " + enabled,
                markerCount(enabled),
                greaterThan(0L)
            );

            setCacheEnabled(false);
            Map<String, List<Map<String, Object>>> disabled = scan(format, "limit_off", tail, stoppedShort());
            assertThat("a disabled cache must write no per-file marker either", disabled, anEmptyMap());
        } finally {
            restoreCacheSetting();
        }
    }

    /**
     * {@code esql.external.cache.enabled} is the only one of the three harvest settings carrying
     * {@code Setting.Property.Dynamic}, so two reads of ONE file can now straddle a toggle. What keeps that safe
     * is the clear on the false edge -- {@code setEnabled(false)} calls {@code clearAll()}. This pins that edge
     * against a single file, which the arms above cannot: each of them deliberately reads its own.
     *
     * <p>The assertions read {@code documentsFound}: a warm fold answers {@code COUNT(*)} without reading a row,
     * so {@code 0} is a served answer and the file's own total is a scan.
     *
     * <p>It does not reach a straddle INSIDE one query -- two splits whose operator factories are built either
     * side of the settings update -- which is a race against cluster-state propagation with no deterministic form
     * here. What bounds that instead is that a half-measured file commits no whole-file count at all:
     * {@code ExternalSourceCacheService.foldStripeFragments} takes {@code lastOrdinal} only from a fragment chain
     * reaching EOF, and {@code ExternalTruncatedReadNeverWarmsIT} pins a truncated read being unservable.
     */
    public void testTogglingTheCacheOffAndOnLeavesTheSameFileCold() throws Exception {
        Path dir = createTempDir();
        writeCsv(dir.resolve("part-0.csv"), false);
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        String count = "FROM " + registerDataset("toggle_csv", dirUri + "*.csv", Map.of()) + " | STATS c = COUNT(*)";
        try {
            setCacheEnabled(true);
            assertCountRowsRead("the first read of the file has nothing to answer from and must scan it", count, ROWS);
            assertCountRowsRead("control: the clean scan's statistics must warm the next COUNT(*)", count, 0);

            setCacheEnabled(false);
            assertCountRowsRead("a disabled cache must serve nothing warm, even for a file already measured", count, ROWS);

            setCacheEnabled(true);
            assertCountRowsRead("turning the cache off cleared it, so the same file must be cold again", count, ROWS);
        } finally {
            restoreCacheSetting();
        }
    }

    /** Runs a {@code COUNT(*)}, asserts the answer is the file's true total, and asserts how it was reached. */
    private void assertCountRowsRead(String reason, String query, long rowsRead) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat("the count must be the file's true total", ((Number) rows.get(0).get(0)).longValue(), equalTo((long) ROWS));
            assertThat(reason, response.documentsFound(), equalTo(rowsRead));
        }
    }

    /** How many captured contributions are the unclean-close marker, as opposed to harvested statistics. */
    private static long markerCount(Map<String, List<Map<String, Object>>> captured) {
        return captured.values()
            .stream()
            .flatMap(List::stream)
            .filter(c -> Boolean.TRUE.equals(c.get(ExternalStats.CHUNK_HAD_ERRORS_KEY)))
            .count();
    }

    private void setCacheEnabled(boolean enabled) {
        updateClusterSettings(Settings.builder().put(ExternalSourceCacheSettings.CACHE_ENABLED.getKey(), enabled));
    }

    private void restoreCacheSetting() {
        updateClusterSettings(Settings.builder().putNull(ExternalSourceCacheSettings.CACHE_ENABLED.getKey()));
    }

    /** The whole file was read: the close drains cleanly and the readers publish. */
    private static Matcher<Long> exhausted() {
        return equalTo((long) ROWS);
    }

    /**
     * The read ended before EOF, which is the non-clean close the marker answers. Deliberately not an exact
     * count: {@code documentsFound} counts rows READ, and a reader stops on a batch boundary once the downstream
     * limiter is satisfied, so the number depends on batch size. What this arm needs is only that the scan did
     * not reach the end of the file -- and that it read something, so a scan that returned nothing for an
     * unrelated reason cannot satisfy this arm.
     */
    private static Matcher<Long> stoppedShort() {
        return both(greaterThan(0L)).and(lessThan((long) ROWS));
    }

    /** Writes one file, scans it, and returns what the read captured, keyed by file path. */
    private Map<String, List<Map<String, Object>>> scan(String format, String name, String tail, Matcher<Long> rowsRead)
        throws IOException {
        Path dir = createTempDir();
        Path file = dir.resolve("part-0." + format);
        switch (format) {
            case "csv" -> writeCsv(file, false);
            case "csv.gz" -> writeCsv(file, true);
            case "ndjson" -> writeNdjson(file);
            default -> throw new IllegalArgumentException("unknown format: " + format);
        }
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        String dataset = registerDataset(name + "_" + format.replace('.', '_'), dirUri + "*." + format, Map.of());

        try (
            EsqlQueryResponse response = run(
                syncEsqlQueryRequest("FROM " + dataset + " " + tail).profile(true),
                TimeValue.timeValueMinutes(5)
            )
        ) {
            assertThat("the read must take the shape this arm is about", response.documentsFound(), rowsRead);
            return capturedSourceMetadata(response);
        }
    }

    /** Pulls the per-driver statistics sink out of the profile, merged across drivers, keyed by file path. */
    private static Map<String, List<Map<String, Object>>> capturedSourceMetadata(EsqlQueryResponse response) {
        Map<String, List<Map<String, Object>>> merged = new HashMap<>();
        for (var status : externalScanStatuses(response)) {
            status.capturedSourceMetadata().forEach((path, list) -> merged.computeIfAbsent(path, k -> new ArrayList<>()).addAll(list));
        }
        return merged;
    }

    private static final String PAD = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    /** A {@link Writer} over a gzip stream, so the fixture is never held whole in memory. */
    private static Writer gzipWriter(Path file) throws IOException {
        OutputStreamWriter w = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(file)), StandardCharsets.UTF_8);
        return new BufferedWriter(w);
    }

    private static void writeCsv(Path file, boolean gzip) throws IOException {
        try (Writer w = gzip ? gzipWriter(file) : Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("id,value,ts,pad\n");
            for (int i = 0; i < ROWS; i++) {
                w.write(i + "," + i + "," + (TS_BASE_MILLIS + i) + "," + PAD + "\n");
            }
        }
    }

    private static void writeNdjson(Path file) throws IOException {
        try (Writer w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (int i = 0; i < ROWS; i++) {
                w.write("{\"id\":" + i + ",\"value\":" + i + ",\"ts\":" + (TS_BASE_MILLIS + i) + ",\"pad\":\"" + PAD + "\"}\n");
            }
        }
    }
}
