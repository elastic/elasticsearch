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
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheTestAccess;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Deterministic PARTIAL-eviction regression tests for the warm multi-file {@code COUNT(*)} fold — the
 * realistic bench failure shape: SOME per-file schema-cache entries are evicted under load while the
 * rest survive, and the all-or-nothing per-file stats merge fails on the FIRST missing file, which
 * (pre-fix) forced the warm query to re-scan every file (the ClickBench 20-minute
 * {@code ndjson,uncompressed,1rg count_hits} warm re-scan).
 * <p>
 * Unlike {@link ExternalNdJsonManyFileWarmFoldIT} (a deliberately tiny cache budget, so LRU decides
 * nondeterministically what survives), this suite runs with a GENEROUS budget where every entry
 * survives on its own, and then surgically invalidates an exact subset — the minimal trigger (exactly
 * one file's entry) and the midpoint (half the files) — via {@link ExternalSourceCacheTestAccess}. A
 * green warm assertion therefore proves the dataset-level aggregate served the count while the
 * per-file merge was failing by construction; it cannot pass through per-file luck. These arms are
 * red on a build without the dataset-level aggregate.
 */
public class ExternalWarmDatasetAggregatePartialEvictionIT extends AbstractWarmDatasetAggregateIT {

    private static final int FILE_COUNT = 40;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class, CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Small stripe grid so every file spans many canonical stripes across multiple read chunks.
            .put("esql.source.cache.stripe.size", "64kb")
            // GENEROUS budget — the opposite end of the spectrum from ExternalNdJsonManyFileWarmFoldIT:
            // every per-file entry fits, so the ONLY missing entries are the ones each test evicts
            // explicitly, making "exactly one file missing" a deterministic construction.
            .put("esql.source.cache.size", "10mb")
            .build();
    }

    public void testNdjsonWarmCountSurvivesOneMissingPerFileEntry() throws Exception {
        runPartialEvictionArm("ndjson", List.of("part-3.ndjson"));
    }

    public void testNdjsonWarmCountSurvivesHalfMissingPerFileEntries() throws Exception {
        List<String> evictions = new ArrayList<>();
        for (int f = 0; f < FILE_COUNT; f += 2) {
            evictions.add("part-" + f + ".ndjson");
        }
        runPartialEvictionArm("ndjson", evictions);
    }

    public void testCsvWarmCountSurvivesOneMissingPerFileEntry() throws Exception {
        runPartialEvictionArm("csv", List.of("part-3.csv"));
    }

    /**
     * Cold scan (materializes the dataset aggregate via the scan's reconcile), surgical invalidation of
     * exactly the given files' schema entries, then warm {@code COUNT(*)}: the total must be correct
     * AND zero documents scanned — the per-file all-or-nothing merge fails on the first missing entry,
     * so only the dataset-level aggregate can have served it.
     */
    private void runPartialEvictionArm(String format, List<String> filesToEvict) throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            Path file = dir.resolve("part-" + f + "." + format);
            total += format.equals("csv") ? writeCsvFile(file, total) : writeNdjsonFile(file, total);
        }
        Map<String, Object> options = format.equals("csv")
            ? Map.of("target_split_size", "256mb")
            : Map.of("segment_size", "64kb", "target_split_size", "256mb");
        String dataset = registerDataset("partial_evict_" + format + "_" + filesToEvict.size(), globUri(dir, "*." + format), options);
        String query = "FROM " + dataset + " | STATS c = COUNT(*)";

        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, total);
            assertThat("cold scan reads all rows", response.documentsFound(), equalTo(total));
        }

        ExternalSourceCacheService cacheService = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        for (String fileName : filesToEvict) {
            // Bounded to exactly one entry per named file: this arm's contract is an EXACT missing
            // subset, and under the generous budget every per-file entry survived the cold query, so
            // each surgical invalidation must remove precisely the file's own entry — otherwise the
            // arm would assert the fallback against a state it never constructed.
            int evicted = ExternalSourceCacheTestAccess.invalidatePerFileSchemaEntries(cacheService, fileName, 1);
            assertThat("surgical eviction of [" + fileName + "] must remove exactly its entry", evicted, equalTo(1));
        }

        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, total);
            assertThat(
                "warm COUNT(*) must survive " + filesToEvict.size() + " missing per-file entr(ies) via the dataset aggregate",
                response.documentsFound(),
                equalTo(0L)
            );
        }
    }

    /**
     * The "cannot use the aggregate" safe-miss guard. The dataset aggregate is row-count-only, so once it
     * is materialized and a per-file entry is evicted (per-file merge fails by construction), a warm
     * {@code COUNT(col)}/{@code MIN}/{@code MAX} must FALL THROUGH to a real scan and return the CORRECT
     * value — it must never fold {@code COUNT(col)} to 0 (the footer implicit-nulls trap) nor serve a
     * stale extremum from the row-count-only map. Column {@code a} runs 0..total-1 globally, so the
     * expected values are exact, and {@code documentsFound == total} proves the query actually scanned.
     */
    public void testWarmNonCountAggregatesSafeMissUnderEviction() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        String dataset = registerDataset(
            "safe_miss_ndjson",
            globUri(dir, "*.ndjson"),
            Map.of("segment_size", "64kb", "target_split_size", "256mb")
        );
        // Cold COUNT(*) materializes the dataset-level aggregate via the scan's reconcile.
        try (var r = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)").profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(r, total);
        }
        // Evict one per-file entry so the per-file all-or-nothing merge fails and only the dataset
        // aggregate (row-count-only) is available.
        ExternalSourceCacheService cacheService = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        assertThat(ExternalSourceCacheTestAccess.invalidatePerFileSchemaEntries(cacheService, "part-3.ndjson", 1), equalTo(1));

        String query = "FROM " + dataset + " | STATS c = COUNT(a), lo = MIN(a), hi = MAX(a)";
        try (var r = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(r);
            assertThat(
                "COUNT(col) must scan to the correct total, never fold to 0",
                ((Number) rows.get(0).get(0)).longValue(),
                equalTo(total)
            );
            assertThat("MIN(a) served from scan", ((Number) rows.get(0).get(1)).longValue(), equalTo(0L));
            assertThat("MAX(a) served from scan", ((Number) rows.get(0).get(2)).longValue(), equalTo(total - 1));
            assertThat(
                "COUNT(col)/MIN/MAX must safe-miss to a scan — the row-count-only aggregate cannot serve them",
                r.documentsFound(),
                equalTo(total)
            );
        }
    }

}
