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
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * A disabled external-source cache must leave no statistics machinery on the read path.
 *
 * <p>Statistics are harvested for that cache and nothing else: with it disabled,
 * {@code ExternalSourceCacheService.reconcileSourceStatsFromContributions} returns on its first line, so every
 * contribution a scan produces is discarded on the coordinator. Harvesting anyway is scan-path cost spent on a
 * result no one accepts.
 *
 * <p>Two terms, which scale differently and so are asserted separately.
 *
 * <ul>
 *   <li><b>Measurements</b> — per-stripe and whole-file row counts and per-column min/max/null. These scale with
 *       rows scanned, and a clean exhaustive scan is what produces them.</li>
 *   <li><b>The unclean-close marker</b> — one map per FILE, written by whichever parsing coordinator ran when a
 *       read did not drain cleanly. It scales with file count rather than bytes, so it survives any fix scoped to
 *       the measurements. It qualifies measurements for the reconciler, so with nothing to qualify and no
 *       reconcile to read it, it is pure overhead.</li>
 * </ul>
 *
 * <p>Reaching that marker takes care. A {@code LIMIT} pushed INTO the source does NOT reach it: a pushed limit
 * leaves {@code rowLimit != NO_LIMIT}, and {@code AsyncExternalSourceOperatorFactory.openWithParallelism} returns
 * null for that, so no coordinator is built and the serial rail writes no marker. The marker needs a read the
 * source runs uncapped that still stops short — a LIMIT behind a filter the planner cannot push, which installs a
 * downstream observed limiter instead. Hence the {@code EVAL}-derived predicate below, and the pinned parsing
 * parallelism, since a coordinator is only built above one.
 *
 * <p>Each assertion of absence is preceded, in the same test, by the same query against an enabled cache. Without
 * that control an empty sink would also be what a test that stopped exercising the read path looks like.
 *
 * <p>Each arm reads its OWN directory and dataset: a shared one would let a later scan answer from an earlier
 * one's statistics and capture nothing for a reason this test is not about.
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
     * A parsing coordinator is built only above parallelism 1, and the marker is written by a coordinator — so the
     * early-close arms pin it rather than depending on the runner's processor count. A release build rejects
     * pragmas at request validation unless the request opts in, which {@link #scan} does, so this holds in both
     * build types and neither arm has to be skipped.
     */
    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("external_parsing_parallelism", 4).build());
    }

    /**
     * Pin every query to one coordinator, as the sibling warm-fold ITs do. The capture itself is read off the data
     * node's operator status, but the enabled-cache control must be a genuine COLD scan, and what has already been
     * harvested is held per coordinator.
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
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
     * The measurement term. An exhaustive {@code COUNT(*)} is the shape that harvests most reliably: row count is
     * recorded for every scope but {@code none}, so it does not depend on which columns the query projects.
     */
    private void assertCleanScanHarvestOnlyWhenEnabled(String format) throws Exception {
        try {
            setCacheEnabled(true);
            Map<String, List<Map<String, Object>>> enabled = scan(format, "clean_on", "| STATS COUNT(*)", exhausted());
            assertThat("control: an enabled cache must harvest something, or this test proves nothing", enabled, not(anEmptyMap()));

            setCacheEnabled(false);
            Map<String, List<Map<String, Object>>> disabled = scan(format, "clean_off", "| STATS COUNT(*)", exhausted());
            assertThat("a disabled cache must harvest nothing", disabled, anEmptyMap());
        } finally {
            restoreCacheSetting();
        }
    }

    /** The per-file term on the segmented coordinator, which plain uncompressed CSV resolves to. */
    public void testCsvEarlyCloseWritesNoPerFileMarkerWithTheCacheDisabled() throws Exception {
        assertEarlyCloseMarkerOnlyWhenEnabled("csv");
    }

    /** The same on the streaming coordinator: a compressed object cannot be range-split, so it takes that rail. */
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
            // Not merely non-empty: a non-empty map could hold a completed stripe rather than the marker, and
            // then this control would pass without ever reaching the path under test.
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
     * not reach the end of the file.
     */
    private static Matcher<Long> stoppedShort() {
        return lessThan((long) ROWS);
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
                // acceptedPragmaRisks because a release build otherwise rejects the parallelism pragma at request
                // validation, and skipping the arms there would drop the marker coverage on exactly the
                // runners most likely to have one processor.
                syncEsqlQueryRequest("FROM " + dataset + " " + tail).pragmas(getPragmas()).acceptedPragmaRisks(true).profile(true),
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
        for (var profile : response.profile().drivers()) {
            for (var status : profile.operators()) {
                if (status.status() instanceof AsyncExternalSourceOperator.Status external) {
                    external.capturedSourceMetadata()
                        .forEach((path, list) -> merged.computeIfAbsent(path, k -> new ArrayList<>()).addAll(list));
                }
            }
        }
        return merged;
    }

    private static final String PAD = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    private static void writeCsv(Path file, boolean gzip) throws IOException {
        StringBuilder sb = new StringBuilder("id,value,ts,pad\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append(i).append(',').append(i).append(',').append(TS_BASE_MILLIS + i).append(',').append(PAD).append('\n');
        }
        if (gzip) {
            try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(file))) {
                out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            }
        } else {
            Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        }
    }

    private static void writeNdjson(Path file) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            sb.append("{\"id\":")
                .append(i)
                .append(",\"value\":")
                .append(i)
                .append(",\"ts\":")
                .append(TS_BASE_MILLIS + i)
                .append(",\"pad\":\"")
                .append(PAD)
                .append("\"}\n");
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }
}
