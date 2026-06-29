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
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.FORK_V9;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Regression coverage for the COUNT(*)-doubling bug on the streaming-compressed read path
 * (issues #150598, #150620, #150723 and the rest of the NdJsonCompressedFormatSpecIT
 * "expected &lt;100L&gt; but was &lt;200L&gt;" family).
 * <p>
 * The aggregate-metadata cache captures per-chunk row-count stats during a cold scan and serves
 * later {@code COUNT(*)} queries from them. A query that scans one file more than once — each branch
 * of a {@code FORK} is an independent subplan that re-scans the {@code EXTERNAL} source — ships two
 * complete covers of the same path. Each per-chunk contribution carries the byte range it observed,
 * and the coordinator reconciler unions them by range: the two scans observe the same deterministic
 * chunk ranges, so those ranges are counted once. Before the fix the reconciler summed every partial
 * under the path, so the cached row count doubled and the next pushdown-eligible {@code COUNT(*)}
 * returned twice the rows. (The same range-union sums genuinely disjoint ranges, so parallel chunks
 * and macro-split partitions are unaffected — partition sums, duplicate covers dedup.)
 * <p>
 * Why the existing {@link ExternalNdJsonAggregatePushdownIT} missed it: that suite reads
 * <em>uncompressed</em> files with {@code parsing_parallelism = 1} (so no parallel partial path is
 * taken) and only ever runs a <em>single-scan</em> cold-then-warm sequence (one finalized set). This
 * test closes both gaps: a gzip file forces the {@code StreamingParallelParsingCoordinator}, a
 * parallelism &gt; 1 pragma forces per-chunk partials, and a {@code FORK} forces two scans of one
 * file in a single query. The whole sequence is deterministic — it does not depend on test ordering
 * the way the spec-suite flakes did.
 */
public class ExternalNdJsonMultiScanPushdownIT extends AbstractEsqlIntegTestCase {

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(NdJsonDataSourcePlugin.class);
        plugins.add(GzipDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 is required to engage the parallel parser: openWithParallelism()
        // bails out at <= 1, in which case the file is read whole (one WholeFile contribution, which
        // the reconciler already dedups) and the double-count cannot reproduce. With > 1, each scan
        // publishes per-chunk PARTIAL contributions plus a finalize marker — the path the bug lives on.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 4).build());
    }

    /**
     * Pins every query to one coordinator: the reconciled schema cache is per-coordinator, so the
     * cold (poisoning) scan and the warm (victim) query must land on the same node. Mirrors
     * {@link ExternalNdJsonAggregatePushdownIT#run}.
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testMultiScanColdDoesNotDoubleCountWarmCountStar() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("requires FORK", FORK_V9.isEnabled());

        int totalRows = 500;
        Path gzFile = writeGzippedNdJsonFile(totalRows);
        try {
            String uri = StoragePath.fileUri(gzFile);

            // COLD: scan the same compressed file twice in one query. Each FORK branch re-scans the
            // EXTERNAL source, so two finalized partial-chunk sets are captured under one path. The
            // COUNT after the merge sees both branches' rows; documentsFound proves both full scans
            // ran (this also guards the test itself — if FORK ever shared a single scan, this drops to
            // totalRows and we would know the test no longer exercises the double-scan path).
            String forkQuery = "EXTERNAL \"" + uri + "\" | FORK (WHERE id >= 0) (WHERE id >= 0) | STATS rows = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(forkQuery).profile(true))) {
                assertSingleLong(response, "rows", 2L * totalRows);
                assertThat(
                    "FORK must scan the file more than once (guards that this test exercises the double-scan path)",
                    response.documentsFound(),
                    greaterThan((long) totalRows)
                );
            }

            // WARM: a pushdown-eligible COUNT(*) on the same file. It short-circuits to the cached
            // row count. Before the fix the cache held 2 * totalRows (the two finalized sets summed),
            // so this returned 1000 instead of 500 — the exact CI signature.
            String countQuery = "EXTERNAL \"" + uri + "\" | STATS c = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(countQuery).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat("warm execution must short-circuit to LocalSourceExec", response.documentsFound(), equalTo(0L));
            }

            // WARM, filtered: the fileMetadataWildcard family. A data-column filter that matches every
            // row is classified back to the full row count, so this is pushdown-eligible too and reads
            // the same cached entry. Same root cause, same fix — assert it is not doubled.
            String filteredCountQuery = "EXTERNAL \"" + uri + "\" | WHERE value >= 0 | STATS c = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(filteredCountQuery).profile(true))) {
                assertCount(response, totalRows);
            }
        } finally {
            Files.deleteIfExists(gzFile);
        }
    }

    private static void assertCount(EsqlQueryResponse response, long expected) {
        assertSingleLong(response, "c", expected);
    }

    private static void assertSingleLong(EsqlQueryResponse response, String columnName, long expected) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(1));
        assertThat(columns.get(0).name(), equalTo(columnName));
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    private static void assertNoPushdownBypass(EsqlQueryResponse response) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);
        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                assertFalse(
                    "expected no Async* operators on warm execution but found: " + op.operator(),
                    op.operator().startsWith("Async")
                );
            }
        }
    }

    private Path writeGzippedNdJsonFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowCount; i++) {
            sb.append("{\"id\":").append(i).append(",\"name\":\"row_").append(i).append("\",\"value\":").append(i * 10).append("}\n");
        }
        Path tempFile = createTempDir().resolve("multiscan_count_pushdown_test.ndjson.gz");
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(tempFile))) {
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        }
        return tempFile;
    }
}
