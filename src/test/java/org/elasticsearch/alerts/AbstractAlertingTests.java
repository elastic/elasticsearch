/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionState;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.plugin.AlertsPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.TestCluster;
import org.junit.After;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0)
public abstract class AbstractAlertingTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("scroll.size", randomIntBetween(1, 100))
                .put("plugin.types", AlertsPlugin.class.getName())
                .build();
    }

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        // This overwrites the wipe logic of the test cluster to not remove the alerts and alerthistory templates. By default all templates are removed
        // TODO: We should have the notion of a hidden template (like hidden index / type) that only gets removed when specifically mentioned.
        final TestCluster testCluster = super.buildTestCluster(scope, seed);
        return new AlertingWrappingCluster(seed, testCluster);
    }

    @After
    public void clearAlerts() throws Exception {
        // Clear all internal alerting state for the next test method:
        stopAlerting();
        client().admin().indices().prepareDelete(AlertsStore.ALERT_INDEX, AlertActionManager.ALERT_HISTORY_INDEX)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
        startAlerting();
    }

    protected BytesReference createAlertSource(String cron, SearchRequest request, String scriptTrigger) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("schedule", cron);
        builder.field("enable", true);

        builder.field("request");
        AlertUtils.writeSearchRequest(request, builder, ToXContent.EMPTY_PARAMS);

        builder.startObject("trigger");
        builder.startObject("script");
        builder.field("script", scriptTrigger);
        builder.endObject();
        builder.endObject();

        builder.startObject("actions");
        builder.startObject("index");
        builder.field("index", "my-index");
        builder.field("type", "trail");
        builder.endObject();
        builder.endObject();

        return builder.endObject().bytes();
    }

    protected SearchRequest createTriggerSearchRequest(String... indices) {
        SearchRequest request = new SearchRequest(indices);
        request.indicesOptions(AlertUtils.DEFAULT_INDICES_OPTIONS);
        request.searchType(AlertUtils.DEFAULT_SEARCH_TYPE);
        return request;
    }

    protected AlertsClient alertClient() {
        return internalTestCluster().getInstance(AlertsClient.class);
    }

    protected void assertAlertTriggered(final String alertName, final long minimumExpectedAlertActionsWithActionPerformed) throws Exception {
        assertAlertTriggered(alertName, minimumExpectedAlertActionsWithActionPerformed, true);
    }

    protected void assertAlertTriggered(final String alertName, final long minimumExpectedAlertActionsWithActionPerformed, final boolean assertTriggerSearchMatched) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                // The alerthistory index gets created in the background when the first alert fires, so we to check first is this index is created and shards are started
                IndicesExistsResponse indicesExistsResponse = client().admin().indices().prepareExists(AlertActionManager.ALERT_HISTORY_INDEX).get();
                assertThat(indicesExistsResponse.isExists(), is(true));
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                IndexRoutingTable routingTable = state.getRoutingTable().index(AlertActionManager.ALERT_HISTORY_INDEX);
                assertThat(routingTable, notNullValue());
                assertThat(routingTable.allPrimaryShardsActive(), is(true));

                SearchResponse searchResponse = client().prepareSearch(AlertActionManager.ALERT_HISTORY_INDEX)
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("alert_name", alertName)).must(matchQuery("state", AlertActionState.ACTION_PERFORMED.toString())))
                        .get();
                assertThat(searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo(minimumExpectedAlertActionsWithActionPerformed));
                if (assertTriggerSearchMatched) {
                    assertThat((Integer) XContentMapValues.extractValue("response.hits.total", searchResponse.getHits().getAt(0).sourceAsMap()), greaterThanOrEqualTo(1));
                }
            }
        });
    }

    protected long findNumberOfPerformedActions(String alertName) {
        SearchResponse searchResponse = client().prepareSearch(AlertActionManager.ALERT_HISTORY_INDEX)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(boolQuery().must(matchQuery("alert_name", alertName)).must(matchQuery("state", AlertActionState.ACTION_PERFORMED.toString())))
                .get();
        return searchResponse.getHits().getTotalHits();
    }

    protected void assertNoAlertTrigger(final String alertName, final long expectedAlertActionsWithNoActionNeeded) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                // The alerthistory index gets created in the background when the first alert fires, so we to check first is this index is created and shards are started
                IndicesExistsResponse indicesExistsResponse = client().admin().indices().prepareExists(AlertActionManager.ALERT_HISTORY_INDEX).get();
                assertThat(indicesExistsResponse.isExists(), is(true));
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                IndexRoutingTable routingTable = state.getRoutingTable().index(AlertActionManager.ALERT_HISTORY_INDEX);
                assertThat(routingTable, notNullValue());
                assertThat(routingTable.allPrimaryShardsActive(), is(true));

                SearchResponse searchResponse = client().prepareSearch(AlertActionManager.ALERT_HISTORY_INDEX)
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("alert_name", alertName)).must(matchQuery("state", AlertActionState.NO_ACTION_NEEDED.toString())))
                        .get();
                assertThat(searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo(expectedAlertActionsWithNoActionNeeded));
            }
        });
    }

    protected void ensureAlertingStarted() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(alertClient().prepareAlertsStats().get().getAlertManagerStarted(), is(State.STARTED));
            }
        });
    }

    protected void ensureAlertingStopped() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(alertClient().prepareAlertsStats().get().getAlertManagerStarted(), is(State.STOPPED));
            }
        });
    }

    protected void startAlerting() throws Exception {
        alertClient().prepareAlertService().start().get();
        ensureAlertingStarted();
    }

    protected void stopAlerting() throws Exception {
        alertClient().prepareAlertService().stop().get();
        ensureAlertingStopped();
    }

    protected static InternalTestCluster internalTestCluster() {
        return (InternalTestCluster) ((AlertingWrappingCluster) cluster()).testCluster;
    }

    private final class AlertingWrappingCluster extends TestCluster {

        private final TestCluster testCluster;

        private AlertingWrappingCluster(long seed, TestCluster testCluster) {
            super(seed);
            this.testCluster = testCluster;
        }

        @Override
        public void beforeTest(Random random, double transportClientRatio) throws IOException {
            testCluster.beforeTest(random, transportClientRatio);
        }

        @Override
        public void wipe() {
            wipeIndices("_all");
            wipeRepositories();

            if (size() > 0) {
                List<String> templatesToWipe = new ArrayList<>();
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                for (ObjectObjectCursor<String, IndexTemplateMetaData> cursor : state.getMetaData().templates()) {
                    if (cursor.key.equals("alerts") || cursor.key.equals("alerthistory")) {
                        continue;
                    }
                    templatesToWipe.add(cursor.key);
                }
                if (!templatesToWipe.isEmpty()) {
                    wipeTemplates(templatesToWipe.toArray(new String[templatesToWipe.size()]));
                }
            }
        }

        @Override
        public void afterTest() throws IOException {
            testCluster.afterTest();
        }

        @Override
        public Client client() {
            return testCluster.client();
        }

        @Override
        public int size() {
            return testCluster.size();
        }

        @Override
        public int numDataNodes() {
            return testCluster.numDataNodes();
        }

        @Override
        public int numDataAndMasterNodes() {
            return testCluster.numDataAndMasterNodes();
        }

        @Override
        public int numBenchNodes() {
            return testCluster.numBenchNodes();
        }

        @Override
        public InetSocketAddress[] httpAddresses() {
            return testCluster.httpAddresses();
        }

        @Override
        public void close() throws IOException {
            InternalTestCluster _testCluster = (InternalTestCluster) testCluster;
            Set<String> nodes = new HashSet<>(Arrays.asList(_testCluster.getNodeNames()));
            String masterNode = _testCluster.getMasterName();
            nodes.remove(masterNode);

            // First manually stop alerting on non elected master node, this will prevent that alerting becomes active
            // on these nodes
            for (String node : nodes) {
                _testCluster.getInstance(AlertManager.class, node).stop();
            }

            // Then stop alerting on elected master node and wait until alerting has stopped on it.
            AlertManager alertManager = _testCluster.getInstance(AlertManager.class, masterNode);
            alertManager.stop();
            while (alertManager.getState() != State.STOPPED) {}

            // Now when can close nodes, without alerting trying to become active while nodes briefly become master
            // during cluster shutdown.
            testCluster.close();
        }

        @Override
        public void ensureEstimatedStats() {
            testCluster.ensureEstimatedStats();
        }

        @Override
        public boolean hasFilterCache() {
            return testCluster.hasFilterCache();
        }

        @Override
        public String getClusterName() {
            return testCluster.getClusterName();
        }

        @Override
        public Iterator<Client> iterator() {
            return testCluster.iterator();
        }

    }
}
