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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end pin of the contract that a truncated cold read must never warm the statistics cache: after a
 * {@code LIMIT}-truncated pass, the next {@code COUNT(*)} returns the file's exact total AND actually
 * re-scans ({@code documentsFound == total} — it was not served a truncated count), while the warm control
 * proves the clean scan's stats DO warm (so the re-scan assertion is not vacuously satisfiable by a dead
 * cache).
 * <p>
 * Honest label — characterization, not a single-defect discriminator. The truncation-safety here is
 * structurally over-determined, and this test measured that directly: it stayed green under every
 * close-path mutation applied while it was built — dropping the parallel coordinators' close-path poison
 * ({@code CHUNK_HAD_ERRORS_KEY}), dropping the CSV reader's {@code naturallyExhausted} publish gate, and
 * dropping the reader's byte-exactness safe-miss, alone and in combination. The outermost reason is
 * structural: {@link org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture#record} is a
 * no-op on a thread with no bound sink, and a row-limited read's close runs on such a thread, so a
 * truncated publish lands nowhere by construction (a pushed-down limit also forces the serial rail —
 * {@code AsyncExternalSourceOperatorFactory.openWithParallelism} refuses row-limited reads — bypassing the
 * coordinators entirely). What this test therefore defends is the CONTRACT against future code paths that
 * would ship or serve truncated stats (a new close-path publish, a serve gate accepting a partial entry,
 * the clean-scan commit breaking), not any one of today's redundant guards. The reconciler-side discard of
 * poisoned and partial contributions is separately unit-pinned in {@code ExternalSourceCacheServiceTests}
 * and {@code SourceStatsContributionTests}.
 */
public class ExternalTruncatedReadNeverWarmsIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Pin the stripe grid at its 64kb floor and keep the file BELOW it: one split, one stripe. A
            // truncated publish that ever reached the reconciler would then form a COMPLETE cover and commit,
            // so this geometry removes the multi-stripe structural incompleteness from the safety story and
            // leaves the publish-suppression layers as what the contract rests on.
            .put("esql.external.cache.stripe.size", "64kb")
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("external_parsing_parallelism", 1).build());
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        // Pin every query to one coordinator: the stats cache is coordinator-local, so the truncated pass,
        // the verifying COUNT and the warm control must all land on the same node to observe one cache.
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testLimitTruncatedColdReadDoesNotWarmCountStar() throws Exception {
        int totalRows = 3_000; // ~50 KB — under the 64 KB stripe grid: one split, one stripe
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_truncated", StoragePath.fileUri(csvFile), Map.of());

            // Cold truncated pass: the pushed-down row limit stops the reader mid-file, well before EOF.
            // Whatever it measured must not be published — it is a partial count under the file's identity.
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 10").profile(true))) {
                assertThat(getValuesList(response).size(), equalTo(10));
            }

            String count = "FROM " + dataset + " | STATS c = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(count).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.size(), equalTo(1));
                assertThat(
                    "COUNT(*) after a truncated pass must be the file's true total, never the partial count",
                    ((Number) rows.get(0).get(0)).longValue(),
                    equalTo((long) totalRows)
                );
                assertThat(
                    "the count must come from a full re-scan — the truncated pass seeded nothing servable",
                    response.documentsFound(),
                    equalTo((long) totalRows)
                );
            }

            // Positive control: the CLEAN scan above did warm the cache. This is what makes the re-scan
            // assertion meaningful — it proves the first COUNT re-scanned because the truncated pass was
            // suppressed, not because caching is off in this setup.
            try (var response = run(syncEsqlQueryRequest(count).profile(true))) {
                assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo((long) totalRows));
                assertThat("a clean scan's stats must warm the next COUNT(*)", response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    private Path writeCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("id,name,value\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("truncated_read_test.csv");
        Files.writeString(tempFile, sb.toString(), StandardCharsets.UTF_8);
        return tempFile;
    }
}
