/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.fetch.OpenFetchContexts;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsAction;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsResponse;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlStatsAction;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The telemetry of the fetch phase on a cluster: the usage counters of {@code GET _esql/stats} and the APM counter of what
 * the planner did with the fetch phase, and the fetch contexts the nodes report open while a query holds them.
 */
public class EsqlFetchTelemetryIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "fetch_telemetry";
    private static final String FETCHING_QUERY = "FROM " + INDEX + " | SORT ts DESC | LIMIT 3 | KEEP ts, payload";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestTelemetryPlugin.class);
        // holds a query on its data nodes while its contexts are open
        plugins.add(SimplePauseFieldPlugin.class);
        return plugins;
    }

    @Before
    public void createIndexAndResetMeters() throws IOException {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
        SimplePauseFieldPlugin.resetPlugin();
        assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(indexSettings(between(1, 3), 0)).setMapping(pausingMapping()));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int ts = 0; ts < 20; ts++) {
            bulk.add(prepareIndex(INDEX).setSource("ts", ts, "payload", "payload-" + ts));
        }
        assertFalse(bulk.get().hasFailures());
        for (TestTelemetryPlugin plugin : telemetryPlugins()) {
            plugin.resetMeter();
        }
    }

    @After
    public void releaseThePause() {
        SimplePauseFieldPlugin.release();
    }

    public void testAQueryThatFetchesCountsAsApplied() {
        long before = usageCounter("fetch_phase.applied");
        run(fetchPhase(FETCHING_QUERY)).close();
        assertThat(usageCounter("fetch_phase.applied"), equalTo(before + 1));
        assertThat(apmQueries("applied", true), equalTo(1L));
    }

    /**
     * A query that asks for the fetch phase and needs every column before the cut counts with the reason it didn't fetch.
     */
    public void testAQueryWithNothingToFetchCountsWhy() {
        long before = usageCounter("fetch_phase.ineligible_no_deferrable_fields");
        run(fetchPhase("FROM " + INDEX + " | SORT ts DESC | LIMIT 3 | KEEP ts")).close();
        assertThat(usageCounter("fetch_phase.ineligible_no_deferrable_fields"), equalTo(before + 1));
        assertThat(apmQueries("ineligible_no_deferrable_fields", true), equalTo(1L));
    }

    /**
     * Without the pragma and with the cluster setting off, a query leaves the fetch phase alone and nothing counts it.
     */
    public void testAQueryThatLeavesTheFetchPhaseAloneCountsNothing() {
        Map<String, Long> before = fetchPhaseUsageCounters();
        run(syncEsqlQueryRequest(FETCHING_QUERY)).close();
        assertThat(fetchPhaseUsageCounters(), equalTo(before));
        assertThat(apmMeasurements(PlanTelemetryManager.FETCH_PHASE_METRICS), empty());
    }

    /**
     * While a query holds its fetch contexts, the nodes report them in their stats and in the gauge, and once it ends they
     * report none.
     */
    public void testTheNodesReportTheFetchContextsAQueryHolds() throws Exception {
        String pausedQuery = "FROM " + INDEX + " | WHERE pause >= 0 | SORT ts DESC | LIMIT 3 | KEEP ts, payload";
        ActionFuture<EsqlQueryResponse> query = client().execute(EsqlQueryAction.INSTANCE, fetchPhase(pausedQuery));
        boolean paused;
        long inTheStats;
        long inTheGauge;
        try {
            paused = SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);
            inTheStats = openContextsInTheStats();
            inTheGauge = openContextsInTheGauge();
        } finally {
            // the query ends before any assertion, so a failure leaves nothing running for the next test
            SimplePauseFieldPlugin.release();
            query.actionGet(30, TimeUnit.SECONDS).close();
        }
        assertTrue("the data drivers start", paused);
        assertThat(inTheStats, greaterThan(0L));
        assertThat(inTheGauge, greaterThan(0L));
        assertBusy(() -> {
            assertThat(openContextsInTheStats(), equalTo(0L));
            assertThat(openContextsInTheGauge(), equalTo(0L));
        });
    }

    private static EsqlQueryRequest fetchPhase(String query) {
        return syncEsqlQueryRequest(query).pragmas(
            new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_PHASE.getKey(), true).build())
        );
    }

    /**
     * {@code ts} and {@code payload}, and a {@code pause} field whose script waits until {@link SimplePauseFieldPlugin}
     * lets it go on.
     */
    private static XContentBuilder pausingMapping() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("pause");
            mapping.field("type", "long");
            mapping.startObject("script").field("source", "").field("lang", "pause").endObject();
            mapping.endObject();
        }
        mapping.endObject();
        mapping.startObject("properties");
        {
            mapping.startObject("ts").field("type", "long").endObject();
            mapping.startObject("payload").field("type", "keyword").endObject();
        }
        mapping.endObject();
        return mapping.endObject();
    }

    private static List<EsqlStatsResponse.NodeStatsResponse> nodeStats() {
        EsqlStatsRequest request = new EsqlStatsRequest();
        request.includeStats(true);
        return client().execute(EsqlStatsAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS).getNodes();
    }

    /**
     * {@code counter} summed over the nodes. Only the coordinator of a query counts it.
     */
    private static long usageCounter(String counter) {
        return nodeStats().stream().mapToLong(node -> node.getStats().get(counter)).sum();
    }

    private static Map<String, Long> fetchPhaseUsageCounters() {
        Map<String, Long> counters = new TreeMap<>();
        for (EsqlStatsResponse.NodeStatsResponse node : nodeStats()) {
            Map<?, ?> fetchPhase = (Map<?, ?>) node.getStats().toNestedMap().get("fetch_phase");
            fetchPhase.forEach((name, count) -> {
                if (name.equals("open_contexts") == false) {
                    counters.merge((String) name, ((Number) count).longValue(), Long::sum);
                }
            });
        }
        return counters;
    }

    private static long openContextsInTheStats() {
        return usageCounter(TransportEsqlStatsAction.FETCH_PHASE_OPEN_CONTEXTS);
    }

    private static long openContextsInTheGauge() {
        long open = 0;
        for (TestTelemetryPlugin plugin : telemetryPlugins()) {
            plugin.resetMeter();
            plugin.collect();
            for (Measurement measurement : plugin.getLongAsyncGaugeMeasurement(OpenFetchContexts.OPEN_CONTEXTS_GAUGE)) {
                open += measurement.getLong();
            }
        }
        return open;
    }

    /**
     * The queries the APM counter counted with {@code outcome} and {@code success}, over the nodes.
     */
    private static long apmQueries(String outcome, boolean success) {
        Map<String, Object> attributes = Map.of(
            PlanTelemetryManager.FETCH_PHASE_OUTCOME,
            outcome,
            PlanTelemetryManager.FETCH_PHASE_QUERY_SUCCESS,
            success
        );
        return apmMeasurements(PlanTelemetryManager.FETCH_PHASE_METRICS).stream()
            .filter(measurement -> measurement.attributes().equals(attributes))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static List<Measurement> apmMeasurements(String name) {
        List<Measurement> measurements = new ArrayList<>();
        for (TestTelemetryPlugin plugin : telemetryPlugins()) {
            measurements.addAll(plugin.getLongCounterMeasurement(name));
        }
        return measurements;
    }

    private static List<TestTelemetryPlugin> telemetryPlugins() {
        List<TestTelemetryPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(TestTelemetryPlugin.class).forEach(plugins::add);
        }
        return plugins;
    }
}
