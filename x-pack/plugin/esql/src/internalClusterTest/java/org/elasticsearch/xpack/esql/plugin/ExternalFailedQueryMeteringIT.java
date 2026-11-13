/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A query over an external source that fails while data nodes read it is metered with what the data nodes did. They tell the
 * coordinator through a transport response header, see {@code FailedComputeUsageHeader}, because a failure carries nothing.
 * <p>
 * The query is coordinated by a node that has no data role, so it is not given any of the splits: everything is read on the data
 * nodes, and the {@code read_cpu_nanos} the metering listener gets can only have come from them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ExternalFailedQueryMeteringIT extends AbstractExternalDataSourceIT {

    /** More files than data nodes, so that round robin gives every data node some of them. */
    private static final int FILES = 8;
    private static final int ROWS_PER_FILE = 20_000;
    /** Every file has a malformed row after this many rows, so a scan fails whichever data node reads it, after it read some rows. */
    private static final int MALFORMED_ROW = 15_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(EsqlQueryMetricsCollectorIT.EsqlEnterpriseWithCollector.class);
        return plugins;
    }

    @Before
    public void requireLocalSourcesAndResetCollectors() {
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        resetCollectors();
    }

    @After
    public void resetCollectors() {
        EsqlQueryMetricsCollectorIT.lastMetrics = null;
        EsqlQueryMetricsCollectorIT.lastOutcome = null;
        EsqlQueryMetricsCollectorIT.collectorCalls.set(0);
        EsqlQueryMetricsCollectorIT.successOnlyCollectorCalls.set(0);
    }

    public void testWorkOfDataNodesIsMeteredWhenTheQueryFails() throws Exception {
        assumeFalse("Windows has bad timer resolution, metrics are not accurate", Constants.WINDOWS);
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureStableCluster(4);

        Path root = writeFilesWithMalformedRows();
        String dataset = registerDataset("remote_failing_ds", StoragePath.fileUri(root) + "/*.csv", Map.of("format", "csv"));

        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + dataset + " | STATS s = SUM(id)");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        expectThrows(Exception.class, () -> {
            try (var ignored = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT)) {
                // the scan must fail on a malformed row
            }
        });

        assertThat("the metering listener is called once", EsqlQueryMetricsCollectorIT.collectorCalls.get(), equalTo(1));
        assertThat(EsqlQueryMetricsCollectorIT.lastOutcome, equalTo(QueryMetricsListener.Outcome.FAILED));
        Map<String, Long> metrics = EsqlQueryMetricsCollectorIT.lastMetrics;
        assertThat(metrics.get(QueryMetricsListener.PLANNING_CPU_NANOS), greaterThan(0L));
        assertThat(
            "what the data nodes read before they failed reached the coordinator",
            metrics.get(QueryMetricsListener.READ_CPU_NANOS),
            greaterThan(0L)
        );
        assertThat(metrics.get(QueryMetricsListener.DRIVER_NANOS), greaterThan(0L));
    }

    /** CSV files whose header declares an integer column and which have a row that is not an integer. */
    private Path writeFilesWithMalformedRows() throws IOException {
        Path root = createTempDir().resolve("remote_failing");
        Files.createDirectories(root);
        for (int f = 0; f < FILES; f++) {
            StringBuilder body = new StringBuilder("id:integer\n");
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                body.append(i == MALFORMED_ROW ? "not_a_number" : Integer.toString(f * ROWS_PER_FILE + i)).append('\n');
            }
            Files.writeString(root.resolve("part_" + f + ".csv"), body.toString());
        }
        return root;
    }
}
