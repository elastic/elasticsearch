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
 * CSV positive control for the unified byte-range cover stripe model (the counterpart of
 * {@link ExternalNdJsonMultiStripeFoldIT}). Both readers now harvest per-stripe stats through the shared
 * {@code StripeStatsHarvester}; this pins the same tiny 64 KB stripe grid so a ~12 MB CSV file spans many
 * stripes and multiple read chunks, forcing the per-stripe emit + the coordinator's {@code 0..K + EOF}
 * interval-cover fold. A warm aggregate that re-scans (documentsFound != 0) here is the fold failing to
 * reach whole-file completeness — proving the shared model works for the proven CSV reader too.
 */
public class ExternalCsvMultiStripeFoldIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Smallest legal stripe grid (64kb floor): a ~12 MB file spans ~190 stripes AND multiple read
            // chunks -> the per-stripe fold across chunks is exercised instead of the whole-chunk shortcut.
            // NodeScope/restart-only, so it must be a static node setting, not a dynamic cluster update.
            .put("esql.source.cache.stripe.size", "64kb")
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testCountStarWarmShortCircuitsAcrossManyStripes() throws Exception {
        int totalRows = 300_000; // ~12 MB -> ~190 stripes at 64 KB + multiple read chunks
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_multistripe", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat("cold scan reads all rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat(
                    "warm COUNT(*) must short-circuit (0 docs scanned) even when the file spans many stripes",
                    response.documentsFound(),
                    equalTo(0L)
                );
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testMinMaxWarmShortCircuitsAcrossManyStripes() throws Exception {
        int totalRows = 300_000;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_multistripe", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertThat("cold scan reads all rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(0L));
                assertThat(((Number) rows.get(0).get(1)).longValue(), equalTo((long) (totalRows - 1) * 10));
                assertThat(
                    "warm MIN/MAX must short-circuit (0 docs scanned) even when the file spans many stripes",
                    response.documentsFound(),
                    equalTo(0L)
                );
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    private static void assertCount(EsqlQueryResponse response, long expected) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    private Path writeCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("id,name,value\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("multistripe_test.csv");
        Files.writeString(tempFile, sb.toString(), StandardCharsets.UTF_8);
        return tempFile;
    }
}
