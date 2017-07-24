/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.AbstractOldXPackIndicesBackwardsCompatibilityTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndicesStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.resolver.indices.IndexStatsResolver;
import org.hamcrest.Matcher;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

/**
 * Tests for monitoring indexes created before {@link Version#CURRENT}.
 */
//Give ourselves 30 seconds instead of 5 to shut down. Sometimes it takes a while, especially on weak hardware. But we do get there.
@ThreadLeakLingering(linger = 30000)
@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/4314")
public class OldMonitoringIndicesBackwardsCompatibilityTests extends AbstractOldXPackIndicesBackwardsCompatibilityTestCase {

    private final boolean httpExporter = randomBoolean();

    @Override
    public Settings nodeSettings(int ord) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(ord))
                .put(XPackSettings.MONITORING_ENABLED.getKey(), true)
                // Don't clean old monitoring indexes - we want to make sure we can load them
                .put(MonitoringSettings.HISTORY_DURATION.getKey(), TimeValue.timeValueHours(1000 * 365 * 24).getStringRep())
                // Do not start monitoring exporters at startup
                .put(MonitoringSettings.INTERVAL.getKey(), "-1");

        if (httpExporter) {
            /* If we want to test the http exporter we have to create it but disable it. We need to create it so we don't use the default
             * local exporter and we have to disable it because we don't yet know the port we've bound to. We can only get that once
             * Elasticsearch starts so we'll enable the exporter then. */
            settings.put(NetworkModule.HTTP_ENABLED.getKey(), true);
            setupHttpExporter(settings, null);
        }
        return settings.build();
    }

    private void setupHttpExporter(Settings.Builder settings, Integer port) {
        Map<String, String> httpExporter = new HashMap<>();
        httpExporter.put("type", "http");
        httpExporter.put("enabled", port == null ? "false" : "true");
        httpExporter.put("host", "http://localhost:" + (port == null ? "does_not_matter" : port));
        httpExporter.put("auth.username", SecuritySettingsSource.TEST_USER_NAME);
        httpExporter.put("auth.password", SecuritySettingsSource.TEST_PASSWORD);

        settings.putProperties(httpExporter, k -> MonitoringSettings.EXPORTERS_SETTINGS.getKey() + "my_exporter." + k);
    }

    @Override
    protected void checkVersion(Version version) throws Exception {
        try {
            logger.info("--> Start testing version [{}]", version);
            if (httpExporter) {
                // If we're using the http exporter we need to update the port and enable it
                NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().get();
                TransportAddress publishAddress = nodeInfos.getNodes().get(0).getHttp().address().publishAddress();
                InetSocketAddress address = publishAddress.address();
                Settings.Builder settings = Settings.builder();
                setupHttpExporter(settings, address.getPort());

                logger.info("--> Enabling http exporter pointing to [localhost:{}]", address.getPort());
                assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
            }

            // Monitoring can now start to collect new data
            Settings.Builder settings = Settings.builder().put(MonitoringSettings.INTERVAL.getKey(), timeValueSeconds(3).getStringRep());
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());

            // And we wait until data have been indexed locally using either by the local or http exporter
            MonitoringIndexNameResolver.Timestamped resolver = new IndexStatsResolver(MonitoredSystem.ES, Settings.EMPTY);
            MonitoringDoc monitoringDoc = new IndexStatsMonitoringDoc(MonitoredSystem.ES.getSystem(), null, null, System.currentTimeMillis(), null, null);
            final String expectedIndex = resolver.index(monitoringDoc);
            final String indexPattern = resolver.indexPattern();

            logger.info("--> {} Waiting for [{}] to be ready", Thread.currentThread().getName(), expectedIndex);
            assertBusy(() -> {
                assertTrue(client().admin().indices().prepareExists(expectedIndex).get().isExists());

                NumShards numShards = getNumShards(expectedIndex);
                ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth(expectedIndex)
                        .setWaitForActiveShards(numShards.numPrimaries)
                        .get();
                assertThat(clusterHealth.getIndices().get(expectedIndex).getActivePrimaryShards(), equalTo(numShards.numPrimaries));
            });

            SearchResponse firstIndexStats =
                    search(indexPattern, IndexStatsMonitoringDoc.TYPE, greaterThanOrEqualTo(10L));

            // All the other aliases should have been created by now so we can assert that we have the data we saved in the bwc indexes
            SearchResponse firstShards = search(indexPattern, ShardMonitoringDoc.TYPE, greaterThanOrEqualTo(10L));
            SearchResponse firstIndices = search(indexPattern, IndicesStatsMonitoringDoc.TYPE, greaterThanOrEqualTo(3L));
            SearchResponse firstNode = search(indexPattern, NodeStatsMonitoringDoc.TYPE, greaterThanOrEqualTo(3L));
            SearchResponse firstState = search(indexPattern, ClusterStatsMonitoringDoc.TYPE, greaterThanOrEqualTo(3L));

            ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
            final String masterNodeId = clusterStateResponse.getState().getNodes().getMasterNodeId();

            // Verify some stuff about the stuff in the backwards compatibility indexes
            Arrays.stream(firstIndexStats.getHits().getHits()).forEach(hit -> checkIndexStats(version, hit.getSourceAsMap()));
            Arrays.stream(firstShards.getHits().getHits()).forEach(hit -> checkShards(version, hit.getSourceAsMap()));
            Arrays.stream(firstIndices.getHits().getHits()).forEach(hit -> checkIndicesStats(version, hit.getSourceAsMap()));
            Arrays.stream(firstNode.getHits().getHits()).forEach(hit -> checkNodeStats(version, masterNodeId, hit.getSourceAsMap()));
            Arrays.stream(firstState.getHits().getHits()).forEach(hit -> checkClusterState(version, hit.getSourceAsMap()));

            // Create some docs
            indexRandom(true, client().prepareIndex("test-1", "doc", "1").setSource("field", 1),
                    client().prepareIndex("test-2", "doc", "2").setSource("field", 2));

            // Wait for monitoring to accumulate some data about the current cluster
            long indexStatsCount = firstIndexStats.getHits().getTotalHits();
            assertBusy(() -> search(indexPattern, IndexStatsMonitoringDoc.TYPE,
                    greaterThan(indexStatsCount)), 1, TimeUnit.MINUTES);
            assertBusy(() -> search(indexPattern, ShardMonitoringDoc.TYPE,
                    greaterThan(firstShards.getHits().getTotalHits())), 1, TimeUnit.MINUTES);
            assertBusy(() -> search(indexPattern, IndicesStatsMonitoringDoc.TYPE,
                    greaterThan(firstIndices.getHits().getTotalHits())), 1, TimeUnit.MINUTES);
            assertBusy(() -> search(indexPattern, NodeStatsMonitoringDoc.TYPE,
                    greaterThan(firstNode.getHits().getTotalHits())), 1, TimeUnit.MINUTES);
            assertBusy(() -> search(indexPattern, ClusterStatsMonitoringDoc.TYPE,
                    greaterThan(firstState.getHits().getTotalHits())), 1, TimeUnit.MINUTES);

        } finally {
            /* Now we stop monitoring and disable the HTTP exporter. We also delete all data and checks multiple times
                if they have not been re created by some in flight monitoring bulk request */
            internalCluster().getInstances(MonitoringService.class).forEach(MonitoringService::stop);

            Settings.Builder settings = Settings.builder().put(MonitoringSettings.INTERVAL.getKey(), "-1");
            if (httpExporter) {
                logger.info("--> Disabling http exporter after test");
                setupHttpExporter(settings, null);
            }
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());

            logger.info("--> Waiting for indices deletion");
            CountDown retries = new CountDown(10);
            assertBusy(() -> {
                String[] indices = new String[]{".monitoring-*"};
                IndicesExistsResponse existsResponse = client().admin().indices().prepareExists(indices).get();
                if (existsResponse.isExists()) {
                    assertAcked(client().admin().indices().prepareDelete(indices));
                } else {
                    retries.countDown();
                }
                assertThat(retries.isCountedDown(), is(true));
            });
            logger.info("--> End testing version [{}]", version);
        }
    }

    private SearchResponse search(String indexPattern, String type, Matcher<Long> hitCount) {
        SearchResponse response = client().prepareSearch(indexPattern).setTypes(type).get();
        assertThat(response.getHits().getTotalHits(), hitCount);
        return response;
    }

    private void checkIndexStats(final Version version, Map<String, Object> indexStats) {
        checkMonitoringElement(indexStats);
        checkSourceNode(version, indexStats);
        Map<?, ?> stats = (Map<?, ?>) indexStats.get("index_stats");
        assertThat(stats, hasKey("index"));
        Map<?, ?> total = (Map<?, ?>) stats.get("total");
        Map<?, ?> docs = (Map<?, ?>) total.get("docs");
        // These might have been taken before all the documents were added so we can't assert a whole lot about the number
        assertThat((Integer) docs.get("count"), greaterThanOrEqualTo(0));
    }

    private void checkShards(final Version version, Map<String, Object> shards) {
        checkMonitoringElement(shards);
        Map<?, ?> shard = (Map<?, ?>) shards.get("shard");
        assertThat(shard, allOf(hasKey("index"), hasKey("state"), hasKey("primary"), hasKey("node")));
    }

    private void checkIndicesStats(final Version version, Map<String, Object> indicesStats) {
        checkMonitoringElement(indicesStats);
        checkSourceNode(version, indicesStats);
        Map<?, ?> stats = (Map<?, ?>) indicesStats.get("indices_stats");
        Map<?, ?> all = (Map<?, ?>) stats.get("_all");
        Map<?, ?> primaries = (Map<?, ?>) all.get("primaries");
        Map<?, ?> docs = (Map<?, ?>) primaries.get("docs");
        // These might have been taken before all the documents were added so we can't assert a whole lot about the number
        assertThat((Integer) docs.get("count"), greaterThanOrEqualTo(0));
    }

    private void checkNodeStats(final Version version, final String masterNodeId, Map<String, Object> nodeStats) {
        checkMonitoringElement(nodeStats);
        checkSourceNode(version, nodeStats);
        Map<?, ?> stats = (Map<?, ?>) nodeStats.get("node_stats");

        // Those fields are expected in every node stats documents
        Set<String> mandatoryKeys = new HashSet<>();
        mandatoryKeys.add("node_id");
        mandatoryKeys.add("node_master");
        mandatoryKeys.add("mlockall");
        mandatoryKeys.add("indices");
        mandatoryKeys.add("os");
        mandatoryKeys.add("fs");
        mandatoryKeys.add("process");
        mandatoryKeys.add("jvm");
        mandatoryKeys.add("thread_pool");

        // disk_threshold_* fields have been removed in 5.0 alpha5, we only check for them if the
        // current tested version is less than or equal to alpha4. Also, the current master node
        // might have collected its own node stats through the Monitoring plugin, and since it is
        // running under Version.CURRENT there's no chance to find these fields.
        if (version.onOrBefore(Version.V_5_0_0_alpha4)) {
            if (masterNodeId.equals((String) stats.get("node_id")) == false) {
                mandatoryKeys.add("disk_threshold_enabled");
                mandatoryKeys.add("disk_threshold_watermark_high");
            }
        }

        for (String key : mandatoryKeys) {
            assertThat("Expecting [" + key + "] to be present for bwc index in version [" + version + "]", stats, hasKey(key));
        }

        Set<?> keys = new HashSet<>(stats.keySet());
        keys.removeAll(mandatoryKeys);
        assertTrue("Found unexpected fields [" + Strings.collectionToCommaDelimitedString(keys) + "] " +
                "for bwc index in version [" + version + "]", keys.isEmpty());
    }

    private void checkClusterState(final Version version, Map<String, Object> clusterState) {
        checkMonitoringElement(clusterState);
        checkSourceNode(version, clusterState);
        Map<?, ?> stats = (Map<?, ?>) clusterState.get("cluster_state");
        assertThat(stats, allOf(hasKey("status"), hasKey("version"), hasKey("state_uuid"), hasKey("master_node"), hasKey("nodes")));
    }

    private void checkMonitoringElement(Map<String, Object> element) {
        assertThat(element, allOf(hasKey("cluster_uuid"), hasKey("timestamp")));
    }

    private void checkSourceNode(final Version version, Map<String, Object> element) {
        assertThat(element, hasKey("source_node"));
    }
}
