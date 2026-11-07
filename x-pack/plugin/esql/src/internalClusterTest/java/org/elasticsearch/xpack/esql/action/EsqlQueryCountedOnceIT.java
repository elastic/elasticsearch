/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsAction;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsResponse;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.getAsyncResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Pins the rule the ES|QL usage counters depend on: a query is counted once, on the node that received it.
 * {@code EsqlUsageTransportAction} reports {@code _xpack/usage} by summing every node's counters, so counting a query
 * again on a data node, on a remote cluster, on a node serving an ENRICH or LOOKUP JOIN lookup, or when an async
 * query is polled would inflate the totals.
 */
public class EsqlQueryCountedOnceIT extends AbstractEnrichBasedCrossClusterTestCase {

    private static final String SPREAD_INDEX = "spread";
    private static final String FAILING_INDEX = "failing";
    private static final String IP_LOOKUP_INDEX = "ip_lookup";
    private static final int MIN_LOCAL_DATA_NODES = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(FailingFieldPlugin.class);
        return plugins;
    }

    // A failing remote must fail the query rather than be skipped, so the failure path is always taken.
    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, false);
    }

    @Before
    public void setupCountedOnceIndices() {
        InternalTestCluster local = cluster(LOCAL_CLUSTER);
        int missingDataNodes = MIN_LOCAL_DATA_NODES - local.numDataNodes();
        if (missingDataNodes > 0) {
            // Same roles as the cluster's own non-master nodes: ensureAtLeastNumDataNodes would add data-only nodes, which
            // cannot coordinate a cross-cluster query.
            local.startNodes(missingDataNodes, NodeRoles.removeRoles(Set.of(DiscoveryNodeRole.MASTER_ROLE)));
        }
        createSpreadIndex(SPREAD_INDEX, Map.of("properties", Map.of("user", Map.of("type", "keyword"), "host", Map.of("type", "ip"))));
        for (int i = 0; i < 20; i++) {
            client(LOCAL_CLUSTER).prepareIndex(SPREAD_INDEX).setSource("user", "user-" + i, "host", "192.168.1." + i).get();
        }
        client(LOCAL_CLUSTER).admin().indices().prepareRefresh(SPREAD_INDEX).get();

        Map<String, Object> failingMapping = Map.of(
            "runtime",
            Map.of("fail_me", Map.of("type", "long", "script", Map.of("source", "", "lang", FailingFieldPlugin.FAILING_FIELD_LANG)))
        );
        createSpreadIndex(FAILING_INDEX, failingMapping);
        populateFailingIndex(client(LOCAL_CLUSTER));
        assertAcked(client(REMOTE_CLUSTER_1).admin().indices().prepareCreate(FAILING_INDEX).setMapping(failingMapping));
        populateFailingIndex(client(REMOTE_CLUSTER_1));

        for (String cluster : allClusters()) {
            Client client = client(cluster);
            assertAcked(
                client.admin()
                    .indices()
                    .prepareCreate(IP_LOOKUP_INDEX)
                    .setSettings(Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP))
                    .setMapping("ip", "type=keyword", "location", "type=keyword")
            );
            for (int i = 0; i < 10; i++) {
                client.prepareIndex(IP_LOOKUP_INDEX).setSource("ip", "192.168.1." + i, "location", "rack-" + i).get();
            }
            client.admin().indices().prepareRefresh(IP_LOOKUP_INDEX).get();
        }
    }

    public void testQueryOverShardsOnSeveralDataNodesCountsOnce() throws Exception {
        String coordinator = randomLocalNode();
        assertCountedOnce(
            coordinator,
            true,
            Set.of(FeatureMetric.FROM, FeatureMetric.STATS),
            () -> runQuery(coordinator, "FROM " + SPREAD_INDEX + " | STATS c = COUNT(*) BY user")
        );
    }

    public void testCrossClusterQueryCountsOnceOnLocalCluster() throws Exception {
        for (String from : List.of(SPREAD_INDEX + ",*:events", "*:events")) {
            String coordinator = randomLocalNode();
            assertCountedOnce(
                coordinator,
                true,
                Set.of(FeatureMetric.FROM, FeatureMetric.STATS),
                () -> runQuery(coordinator, "FROM " + from + " | STATS c = COUNT(*) BY user")
            );
        }
    }

    public void testLookupJoinCountsOnce() throws Exception {
        for (String from : List.of(SPREAD_INDEX, SPREAD_INDEX + ",*:events")) {
            String coordinator = randomLocalNode();
            assertCountedOnce(
                coordinator,
                true,
                Set.of(FeatureMetric.FROM, FeatureMetric.EVAL, FeatureMetric.LOOKUP_JOIN, FeatureMetric.STATS),
                () -> runQuery(
                    coordinator,
                    "FROM "
                        + from
                        + " | EVAL ip = TO_STR(host) | LOOKUP JOIN "
                        + IP_LOOKUP_INDEX
                        + " ON ip | STATS c = COUNT(*) BY location"
                )
            );
        }
    }

    public void testEnrichCountsOnce() throws Exception {
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            String coordinator = randomLocalNode();
            assertCountedOnce(
                coordinator,
                true,
                Set.of(FeatureMetric.FROM, FeatureMetric.EVAL, FeatureMetric.ENRICH, FeatureMetric.STATS),
                () -> runQuery(
                    coordinator,
                    "FROM "
                        + SPREAD_INDEX
                        + ",*:events | EVAL ip = TO_STR(host) | ENRICH _"
                        + mode.name().toLowerCase(Locale.ROOT)
                        + ":hosts ON ip | STATS c = COUNT(*) BY os"
                )
            );
        }
    }

    public void testAsyncQueryCountsOnceRegardlessOfPolling() throws Exception {
        String coordinator = randomLocalNode();
        assertCountedOnce(coordinator, true, Set.of(FeatureMetric.FROM, FeatureMetric.STATS), () -> {
            EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest("FROM " + SPREAD_INDEX + ",*:events | STATS c = COUNT(*)");
            request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
            request.waitForCompletionTimeout(timeValueMillis(randomIntBetween(1, 100)));
            request.keepOnCompletion(true);
            String asyncExecutionId;
            try (EsqlQueryResponse response = executeOn(coordinator, request)) {
                assertThat(response.asyncExecutionId().isPresent(), is(true));
                asyncExecutionId = response.asyncExecutionId().get();
            }
            assertBusy(() -> {
                try (EsqlQueryResponse response = getAsyncResponse(cluster(LOCAL_CLUSTER).client(coordinator), asyncExecutionId)) {
                    assertThat(response.isRunning(), is(false));
                }
            });
            int polls = randomIntBetween(2, 5);
            for (int i = 0; i < polls; i++) {
                try (EsqlQueryResponse response = getAsyncResponse(cluster(LOCAL_CLUSTER).client(randomLocalNode()), asyncExecutionId)) {
                    assertThat(response.isRunning(), is(false));
                }
            }
            assertAcked(deleteAsyncId(cluster(LOCAL_CLUSTER).client(coordinator), asyncExecutionId));
        });
    }

    public void testStreamQueryCountsOnce() throws Exception {
        String coordinator = randomLocalNode();
        assertCountedOnce(coordinator, true, Set.of(FeatureMetric.FROM, FeatureMetric.STATS), () -> {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(
                "FROM " + SPREAD_INDEX + ",*:events | STATS c = COUNT(*) BY user"
            );
            request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
            StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
            StreamQueryTestUtils.executeStreamRequest(cluster(LOCAL_CLUSTER).client(coordinator), request, subscriber);
        });
    }

    public void testQueryFailingAnalysisCountsOnceAsFailed() throws Exception {
        String coordinator = randomLocalNode();
        assertCountedOnce(coordinator, false, Set.of(), () -> {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest("FROM " + SPREAD_INDEX + ",*:events | EVAL x = no_such_field");
            Exception e = expectThrows(VerificationException.class, () -> executeOn(coordinator, request).close());
            assertThat(e.getMessage(), containsString("Unknown column [no_such_field]"));
        });
    }

    public void testQueryFailingOnDataNodesCountsOnceAsFailed() throws Exception {
        for (String from : List.of(FAILING_INDEX, REMOTE_CLUSTER_1 + ":" + FAILING_INDEX)) {
            String coordinator = randomLocalNode();
            assertCountedOnce(coordinator, false, Set.of(FeatureMetric.FROM, FeatureMetric.STATS), () -> {
                EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest("FROM " + from + " | STATS s = SUM(fail_me)");
                request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
                request.allowPartialResults(false);
                Exception e = expectThrows(Exception.class, () -> executeOn(coordinator, request).close());
                assertThat(ExceptionsHelper.stackTrace(e), containsString("Accessing failing field"));
            });
        }
    }

    /**
     * Runs {@code query}, which must send exactly one ES|QL query to {@code coordinator}, and checks that the query was
     * counted once, by the coordinator alone: in the summed {@code _xpack/usage} counters of the local cluster, in each
     * node's counters, in every remote cluster's counters, and in the APM counters.
     */
    private void assertCountedOnce(String coordinator, boolean success, Set<FeatureMetric> features, CheckedRunnable<Exception> query)
        throws Exception {
        Map<String, Map<String, Counters>> before = statsByClusterAndNode();
        resetTelemetry();

        query.run();

        Map<String, Map<String, Counters>> after = statsByClusterAndNode();
        Counters localBefore = Counters.merge(List.copyOf(before.get(LOCAL_CLUSTER).values()));
        Counters localAfter = Counters.merge(List.copyOf(after.get(LOCAL_CLUSTER).values()));
        for (String name : List.of("queries._all.total", "queries.rest.total")) {
            assertThat(name, delta(localBefore, localAfter, name), is(1L));
        }
        for (String name : List.of("queries._all.failed", "queries.rest.failed")) {
            assertThat(name, delta(localBefore, localAfter, name), is(success ? 0L : 1L));
        }
        assertThat("took", tookCount(localAfter) - tookCount(localBefore), is(success ? 1L : 0L));
        for (FeatureMetric feature : FeatureMetric.values()) {
            String name = "features." + feature;
            assertThat(name, delta(localBefore, localAfter, name), is(features.contains(feature) ? 1L : 0L));
        }

        for (Map.Entry<String, Map<String, Counters>> cluster : after.entrySet()) {
            for (Map.Entry<String, Counters> node : cluster.getValue().entrySet()) {
                boolean isCoordinator = cluster.getKey().equals(LOCAL_CLUSTER) && node.getKey().equals(coordinator);
                if (isCoordinator == false) {
                    String where = cluster.getKey() + "/" + node.getKey();
                    assertThat(where, node.getValue().toNestedMap(), is(before.get(cluster.getKey()).get(node.getKey()).toNestedMap()));
                }
            }
        }

        assertCommandsPublishedOnceByCoordinator(coordinator, success);
    }

    /**
     * Every command a query uses adds one to {@link PlanTelemetryManager#FEATURE_METRICS}, so after one query each
     * measurement is exactly 1, and only the coordinator has any.
     */
    private void assertCommandsPublishedOnceByCoordinator(String coordinator, boolean success) {
        for (String cluster : allClusters()) {
            for (String node : cluster(cluster).getNodeNames()) {
                List<Measurement> commands = Measurement.combine(
                    telemetryPlugin(cluster, node).getLongCounterMeasurement(PlanTelemetryManager.FEATURE_METRICS)
                );
                if (cluster.equals(LOCAL_CLUSTER) && node.equals(coordinator)) {
                    Set<Object> names = new HashSet<>();
                    for (Measurement measurement : commands) {
                        Object name = measurement.attributes().get(PlanTelemetryManager.FEATURE_NAME);
                        names.add(name);
                        assertThat(PlanTelemetryManager.FEATURE_METRICS + " " + name, measurement.getLong(), is(1L));
                        assertThat(measurement.attributes().get(PlanTelemetryManager.SUCCESS), is(success));
                    }
                    assertThat(names, hasItem("FROM"));
                } else {
                    assertThat(cluster + "/" + node, commands, is(empty()));
                }
            }
        }
    }

    private Map<String, Map<String, Counters>> statsByClusterAndNode() {
        Map<String, Map<String, Counters>> stats = new HashMap<>();
        for (String cluster : allClusters()) {
            EsqlStatsRequest request = new EsqlStatsRequest();
            request.includeStats(true);
            EsqlStatsResponse response = client(cluster).execute(EsqlStatsAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
            assertThat(response.failures(), is(empty()));
            Map<String, Counters> byNode = new HashMap<>();
            for (EsqlStatsResponse.NodeStatsResponse node : response.getNodes()) {
                assertThat(node.getStats(), notNullValue());
                byNode.put(node.getNode().getName(), node.getStats());
            }
            assertThat(byNode.keySet(), is(Set.of(cluster(cluster).getNodeNames())));
            stats.put(cluster, byNode);
        }
        return stats;
    }

    private void resetTelemetry() {
        for (String cluster : allClusters()) {
            for (String node : cluster(cluster).getNodeNames()) {
                telemetryPlugin(cluster, node).resetMeter();
            }
        }
    }

    private TestTelemetryPlugin telemetryPlugin(String cluster, String node) {
        List<TestTelemetryPlugin> plugins = cluster(cluster).getInstance(PluginsService.class, node)
            .filterPlugins(TestTelemetryPlugin.class)
            .toList();
        assertThat(plugins, hasSize(1));
        return plugins.getFirst();
    }

    private static long delta(Counters before, Counters after, String name) {
        return after.get(name) - before.get(name);
    }

    @SuppressWarnings("unchecked")
    private static long tookCount(Counters counters) {
        Map<String, Object> took = (Map<String, Object>) counters.toNestedMap().get("took");
        return took.values().stream().mapToLong(v -> (Long) v).sum();
    }

    private void createSpreadIndex(String index, Map<String, Object> mapping) {
        int dataNodes = cluster(LOCAL_CLUSTER).numDataNodes();
        CreateIndexResponse response = client(LOCAL_CLUSTER).admin()
            .indices()
            .prepareCreate(index)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", dataNodes)
                    .put("index.number_of_replicas", 0)
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
            )
            .setMapping(mapping)
            .get();
        assertThat(response.isShardsAcknowledged(), is(true));
        IndexRoutingTable routing = client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(index);
        Set<String> nodesWithShards = new HashSet<>();
        for (int shard = 0; shard < routing.size(); shard++) {
            nodesWithShards.add(routing.shard(shard).primaryShard().currentNodeId());
        }
        assertThat(nodesWithShards, hasSize(dataNodes));
    }

    private static void populateFailingIndex(Client client) {
        for (int i = 0; i < 10; i++) {
            client.prepareIndex(FAILING_INDEX).setSource("id", i).get();
        }
        client.admin().indices().prepareRefresh(FAILING_INDEX).get();
    }

    private String randomLocalNode() {
        List<String> coordinators = cluster(LOCAL_CLUSTER).clusterService()
            .state()
            .nodes()
            .stream()
            .filter(node -> node.canContainData() && node.getRoles().contains(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
            .map(DiscoveryNode::getName)
            .toList();
        return randomFrom(coordinators);
    }

    private void runQuery(String coordinator, String query) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        executeOn(coordinator, request).close();
    }

    private EsqlQueryResponse executeOn(String coordinator, EsqlQueryRequest request) {
        return cluster(LOCAL_CLUSTER).client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }
}
