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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Regression tests for the ClickBench {@code ndjson,uncompressed,1rg count_hits} pathology: a warm
 * {@code FROM <many-file-glob> | STATS COUNT(*)} that RE-SCANS every file instead of serving the
 * cached row count. The warm multi-file fold is all-or-nothing (the per-file stats aggregate fails the
 * moment ONE file lacks {@code _stats.row_count}), so under schema-cache LRU pressure — simulated here
 * with a deliberately tiny {@code esql.external.cache.size} — a single evicted per-file entry used to
 * force every file to re-scan. The fix is the DATASET-LEVEL row-count aggregate: a single entry keyed
 * by the listing's file-set fingerprint (path+mtime+size of every file), materialized by the cold scan's
 * reconcile and served when the per-file merge comes back incomplete, so warm {@code COUNT(*)} needs
 * exactly one cache survival instead of N.
 * <p>
 * The CSV arm is the control (same shape, same fix); the surgical arm below removes ONE per-file entry
 * explicitly, making the failure mode deterministic instead of load-dependent; the file-set mutation
 * arm proves listing-cache TTL hides a mutation, then after listing expiry the fingerprint
 * is correct-or-miss (re-scan, new truth, re-warm).
 */
public class ExternalNdJsonManyFileWarmFoldIT extends AbstractWarmDatasetAggregateIT {

    // Enough files that at least one straggler (incomplete stripe coverage) is near-certain, mirroring the
    // ClickBench cell where the ~50-file glob warmed but the 226-file glob did not.
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
            .put("esql.external.cache.stripe.size", "64kb")
            // Simulate the cache pressure a loaded cluster creates: a tiny budget so the 40 per-file
            // entries cannot all coexist, forcing LRU eviction of already-committed siblings.
            .put("esql.external.cache.size", "48kb")
            .build();
    }

    public void testNdjsonManyFileCountStarWarmShortCircuits() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        String dataset = registerDataset(
            "ndjson_manyfile",
            globUri(dir, "*.ndjson"),
            Map.of("segment_size", "64kb", "target_split_size", "256mb")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    /**
     * The deterministic arm: after the cold scan materializes the dataset aggregate, surgically evict
     * EVERY per-file schema entry of the dataset (the worst case LRU pressure can produce — the tiny
     * suite budget may already have evicted an arbitrary subset, so evicting the remainder is the only
     * deterministic construction). The per-file all-or-nothing merge then fails by construction, so a
     * green warm assertion proves the dataset-level fallback — not per-file luck — served the count.
     */
    public void testNdjsonWarmCountSurvivesPerFileEntryEviction() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        String dataset = registerDataset(
            "ndjson_evict_all",
            globUri(dir, "*.ndjson"),
            Map.of("segment_size", "64kb", "target_split_size", "256mb")
        );
        String query = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, total);
        }
        ExternalSourceCacheService cacheService = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        // Evict by directory substring so the count is deterministic regardless of what the tiny LRU
        // budget already dropped; the dataset-aggregate entry, which its file-set fingerprint identifies, is deliberately spared.
        ExternalSourceCacheTestAccess.invalidatePerFileSchemaEntries(cacheService, dir.getFileName().toString());
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, total);
            assertThat(
                "warm COUNT(*) must survive per-file entry eviction via the dataset aggregate",
                response.documentsFound(),
                equalTo(0L)
            );
        }
    }

    /**
     * Two layers, in order. The dataset aggregate is keyed by the listing's file-set fingerprint, so
     * a listing that actually changed (add or mtime touch) must miss it, re-scan, and re-warm. The
     * listing cache sits in front of that. TestValidator omit-key hydrates {@code union_by_name};
     * the listing is still reused for the TTL, so a mutation is invisible until the listing expires.
     * Homogeneous files: {@code COUNT(*)} folds on either rail. This arm asserts both — TTL-hot
     * listing keeps the old count, then a listing miss (TTL expiry, simulated by dropping listing
     * entries only) sees the live set. Serving a new count while the listing is still cached, or a
     * stale count after it has expired, is the failure this exists to catch.
     */
    public void testNdjsonFileSetChangeInvalidatesDatasetAggregate() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        String dataset = registerDataset(
            "ndjson_mutate_set",
            globUri(dir, "*.ndjson"),
            Map.of("segment_size", "64kb", "target_split_size", "256mb")
        );
        assertWarmCountShortCircuits(dataset, total);

        ExternalSourceCacheService cacheService = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        String query = "FROM " + dataset + " | STATS c = COUNT(*)";

        long newTotal = total + writeNdjsonFile(dir.resolve("part-added.ndjson"), total);
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, total);
            assertThat(
                "TTL-hot listing must keep the pre-add fingerprint and serve the warm aggregate",
                response.documentsFound(),
                equalTo(0L)
            );
        }
        ExternalSourceCacheTestAccess.invalidateListings(cacheService);
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, newTotal);
            assertThat("after listing expiry a grown set must re-scan", response.documentsFound(), equalTo(newTotal));
        }
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, newTotal);
            assertThat("the grown set must warm again after one scan", response.documentsFound(), equalTo(0L));
        }

        // A moved mtime is a new version of a local file, even with the same bytes: the listing still names the old one, so
        // resolution's read of the touched file fails with the object-changed condition. That failure is caught during
        // resolution, before anything has run, so resolution drops the listing and lists again: the query re-scans.
        Path touched = dir.resolve("part-0.ndjson");
        Files.setLastModifiedTime(touched, FileTime.fromMillis(Files.getLastModifiedTime(touched).toMillis() + 2_000));
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, newTotal);
            assertThat("resolution re-lists the touched set, so the query re-scans it", response.documentsFound(), equalTo(newTotal));
        }
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, newTotal);
            assertThat("the touched set must warm again after one scan", response.documentsFound(), equalTo(0L));
        }
    }

    /**
     * A partition filter lists the dataset under its own filter-hint key. A rewrite of a file under that filter fails the first read
     * with the object-changed condition, and the failure drops the cached listings of the dataset. The next query lists again and
     * returns the rewritten file's rows.
     */
    public void testRewrittenFileUnderPartitionFilterFailsThenReadsNewRows() throws Exception {
        Path dir = createTempDir();
        Path y2024 = Files.createDirectories(dir.resolve("year=2024"));
        Path y2025 = Files.createDirectories(dir.resolve("year=2025"));
        long part0Rows = writeNdjsonFile(y2024.resolve("part-0.ndjson"), 0);
        long part1Rows = writeNdjsonFile(y2024.resolve("part-1.ndjson"), part0Rows);
        writeNdjsonFile(y2025.resolve("part-0.ndjson"), 1_000_000L);
        String dataset = registerDataset(
            "ndjson_partition_rewrite",
            globUri(dir, "*/*.ndjson"),
            Map.of("segment_size", "64kb", "target_split_size", "256mb")
        );
        String query = "FROM " + dataset + " | WHERE year == 2024 | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, part0Rows + part1Rows);
        }

        // A rewrite with two rows: a new length and a new modification time, so the listed version is stale.
        Files.writeString(y2024.resolve("part-0.ndjson"), "{\"a\":0,\"b\":0}\n{\"a\":1,\"b\":10}\n");
        Exception changed = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5)).close()
        );
        // The read ran on a data node, so the failure arrives as a wrapper that keeps the condition, not the exception class.
        assertTrue(
            "the first read under the stale listing must fail with the object-changed condition",
            ExternalException.hasCondition(changed, ExternalException.Condition.OBJECT_CHANGED)
        );
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, 2L + part1Rows);
        }
    }

    public void testCsvManyFileCountStarWarmShortCircuits() throws Exception {
        // Control: the same many-file warm-fold shape on CSV must short-circuit. If this passes and the
        // NDJSON arm fails, the defect is NDJSON-specific (not the all-or-nothing fold in the abstract).
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeCsvFile(dir.resolve("part-" + f + ".csv"), total);
        }
        String dataset = registerDataset("csv_manyfile", globUri(dir, "*.csv"), Map.of("target_split_size", "256mb"));
        assertWarmCountShortCircuits(dataset, total);
    }

    /**
     * Same warm {@code COUNT(*)} fold on a {@code local} data source, so omitted
     * {@code schema_resolution} persists as {@code first_file_wins}. Homogeneous files fold on
     * either rail; this pins the product default path the TestValidator omit-key tests skip.
     */
    public void testNdjsonWarmCountShortCircuitsOnPersistedFirstFileWins() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < 4; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        String dataset = registerLocalFileDataset(
            "ndjson_ffw_warm",
            globUri(dir, "*.ndjson"),
            Map.of("segment_size", "64kb", "target_split_size", "256mb")
        );
        assertWarmCountShortCircuits(dataset, total);
    }

    private void assertWarmCountShortCircuits(String dataset, long total) {
        String query = "FROM " + dataset + " | STATS c = COUNT(*)";
        // Cold: reads every row across every file.
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, total);
            assertThat("cold scan reads all rows", response.documentsFound(), equalTo(total));
        }
        // Warm: must serve the cached fold with ZERO documents scanned across ALL files.
        try (var response = run(syncEsqlQueryRequest(query).profile(true), TimeValue.timeValueMinutes(5))) {
            assertCount(response, total);
            assertThat("warm COUNT(*) over a many-file glob must short-circuit (0 docs scanned)", response.documentsFound(), equalTo(0L));
        }
    }

}
