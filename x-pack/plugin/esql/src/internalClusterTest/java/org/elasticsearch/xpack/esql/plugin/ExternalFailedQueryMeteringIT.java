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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

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
    /** A file big enough to take many reads, whose second column has a malformed value near the start: a sub-plan reads the first. */
    private static final int SUBPLAN_ROWS = 400_000;
    private static final int SUBPLAN_MALFORMED_ROW = 50;

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
        String coordinator = startClusterWithDataNodes();

        Path root = writeFilesWithMalformedRows();
        String dataset = registerDataset("remote_failing_ds", StoragePath.fileUri(root) + "/*.csv", Map.of("format", "csv"));

        expectThrows(Exception.class, () -> runOn(coordinator, "FROM " + dataset + " | STATS s = SUM(id)"));

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

    /**
     * A sub-plan that completed before the query failed did work that only the session knows about, because a failure carries no
     * completion info. The query below runs the sub-plan of {@code INLINE STATS}, which reads the first column, and then fails in the
     * main plan on a malformed value in the second one, which the sub-plan never reads. It uses the result of the sub-plan, or the
     * optimizer would drop it, and groups it: without a grouping the optimizer folds it into the main plan and there is no sub-plan.
     * <p>
     * The bytes read are what tells the work of the sub-plan from the one of the failed compute: the sub-plan reads the whole file, and
     * the main plan fails near its start. So the usage is that of the sub-plan once, or {@code B}, plus a part of what the main plan
     * would read, which is less than another {@code B}: a sub-plan that was lost leaves less than {@code B}, one that was counted
     * twice leaves {@code 2B} or more. {@code B} is what the sub-plan alone reads, from a query that succeeds.
     */
    public void testWorkOfCompletedSubPlanIsMeteredOnceWhenTheQueryFails() throws Exception {
        assumeFalse("Windows has bad timer resolution, metrics are not accurate", Constants.WINDOWS);
        assumeTrue("requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        String coordinator = startClusterWithDataNodes();

        Path root = createTempDir().resolve("subplan_failing");
        Files.createDirectories(root);
        StringBuilder body = new StringBuilder("id:integer,n:integer\n");
        for (int i = 0; i < SUBPLAN_ROWS; i++) {
            body.append(i).append(',').append(i == SUBPLAN_MALFORMED_ROW ? "not_a_number" : Integer.toString(i)).append('\n');
        }
        Files.writeString(root.resolve("data.csv"), body.toString());
        String dataset = registerDataset("subplan_failing_ds", StoragePath.fileUri(root) + "/*.csv", Map.of("format", "csv"));

        // what the sub-plan reads on its own, from a query that succeeds: this also shows that it does not read the malformed column
        runOn(coordinator, "FROM " + dataset + " | STATS m = MAX(id)").close();
        assertThat(EsqlQueryMetricsCollectorIT.lastOutcome, equalTo(QueryMetricsListener.Outcome.SUCCESS));
        long subPlanBytes = EsqlQueryMetricsCollectorIT.lastMetrics.get(QueryMetricsListener.BYTES_READ);
        assertThat(subPlanBytes, greaterThan(0L));
        resetCollectors();

        expectThrows(
            Exception.class,
            () -> runOn(coordinator, "FROM " + dataset + " | INLINE STATS m = MAX(id) BY g = id % 10 | STATS s = SUM(n + m)")
        );

        assertThat("the metering listener is called once", EsqlQueryMetricsCollectorIT.collectorCalls.get(), equalTo(1));
        assertThat(EsqlQueryMetricsCollectorIT.lastOutcome, equalTo(QueryMetricsListener.Outcome.FAILED));
        long bytesRead = EsqlQueryMetricsCollectorIT.lastMetrics.get(QueryMetricsListener.BYTES_READ);
        assertThat("the work of the sub-plan that completed is not lost", bytesRead, greaterThanOrEqualTo(subPlanBytes));
        assertThat("the work of the sub-plan that completed is not counted twice", bytesRead, lessThan(2 * subPlanBytes));
    }

    private String startClusterWithDataNodes() {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureStableCluster(4);
        return coordinator;
    }

    /** Runs {@code query} on {@code node}, reading the external source remotely whichever data node gets the files. */
    private EsqlQueryResponse runOn(String node, String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        return client(node).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT);
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
