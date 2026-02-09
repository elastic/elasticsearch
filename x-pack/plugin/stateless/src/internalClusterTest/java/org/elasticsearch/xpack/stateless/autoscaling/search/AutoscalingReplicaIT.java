/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopology;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredClusterTopology.TierTopology;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredTopologyContext;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class AutoscalingReplicaIT extends AbstractStatelessPluginIntegTestCase {

    private static final long DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(7).millis();
    private static final long ONE_DAY = TimeValue.timeValueDays(1).millis();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    private static void scaleReplicasUp(String indexName) throws Exception {
        assertThat(getNumberOfReplicas(indexName), equalTo(1));

        int replicationSearchPower = ReplicasUpdaterService.SEARCH_POWER_MIN_FULL_REPLICATION;
        assertAcked(
            clusterAdmin().updateSettings(
                new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                    Settings.builder()
                        .put(
                            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(),
                            randomIntBetween(replicationSearchPower, replicationSearchPower + 100)
                        )
                )
            )
        );
        assertBusy(() -> assertThat(getNumberOfReplicas(indexName), equalTo(2)));
    }

    private static void scaleReplicasDown(String indexName) throws Exception {
        assertAcked(
            clusterAdmin().updateSettings(
                new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                    Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 100)
                )
            )
        );
        assertBusy(() -> assertThat(getNumberOfReplicas(indexName), equalTo(1)));
    }

    private static int getNumberOfReplicas(String indexName) {
        return clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
            .actionGet()
            .getState()
            .metadata()
            .getProject()
            .index(indexName)
            .getNumberOfReplicas();
    }

    public void testSearchPowerAffectsReplica() throws Exception {
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
            .build();
        startMasterOnlyNode(settings);

        boolean hollowShards = randomBoolean();
        Settings indexNodeSettings = hollowShards
            ? Settings.builder()
                .put(settings)
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
                .put(HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
                .build()
            : settings;
        String indexNodeA = startIndexNode(indexNodeSettings);
        String indexNodeB = startIndexNode(indexNodeSettings);
        startSearchNode(settings);
        // Need to start a second search node for index2 before a relocation because needs 2 replicas
        startSearchNode(settings);

        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put("index.routing.allocation.exclude._name", indexNodeB).build());

        // new documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            100,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        if (hollowShards) {
            flushAndRefresh(indexName);
            hollowShards(indexName, 1, indexNodeA, indexNodeB);
        } else {
            refresh(indexName);
        }
        scaleReplicasUp(indexName);

        // also check that a newly created index gets scaled up automatically
        var indexName2 = randomIdentifier();
        createIndex(indexName2, indexSettings(5, 1).put("index.routing.allocation.exclude._name", indexNodeB).build());
        assertEquals(1, getNumberOfReplicas(indexName2));

        indexDocumentsWithTimestamp(
            indexName2,
            1,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName2);
        waitUntil(() -> getNumberOfReplicas(indexName2) == 2, 5, TimeUnit.SECONDS);
        assertEquals(2, getNumberOfReplicas(indexName2));

        if (randomBoolean() && hollowShards) {
            hollowShards(indexName2, 5, indexNodeA, indexNodeB);
            assertEquals(2, getNumberOfReplicas(indexName2));
        }

        // back to SP 100
        scaleReplicasDown(indexName);
    }

    public void testSearchSizeAffectsReplicasSPBetween100And250() throws Exception {
        // start the nodes with replica autoscaling disabled, we switch it on later
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(300))
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false)
            .build();
        startMasterOnlyNode(settings);
        boolean hollowShards = randomBoolean();
        Settings indexNodeSettings = hollowShards
            ? Settings.builder()
                .put(settings)
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
                .put(HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
                .build()
            : settings;
        String indexNodeA = startIndexNode(indexNodeSettings);
        String indexNodeB = startIndexNode(indexNodeSettings);
        startSearchNode(settings);
        // Need to start a second search node for index2 before a relocation because needs 2 replicas
        startSearchNode(settings);

        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        // create two indices, where size index1 is roughly 2/3 and index2 is roughly 1/3 of interactive size
        var index1 = "index1";
        createIndex(index1, indexSettings(1, 1).put("index.routing.allocation.exclude._name", indexNodeB).build());
        var index2 = "index2";
        createIndex(index2, indexSettings(1, 1).put("index.routing.allocation.exclude._name", indexNodeB).build());

        // new documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            index1,
            200,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        if (hollowShards) {
            flushAndRefresh(index1);
            hollowShards(index1, 1, indexNodeA, indexNodeB);
        } else {
            refresh(index1);
        }

        indexDocumentsWithTimestamp(
            index2,
            100,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        if (hollowShards) {
            flushAndRefresh(index2);
            hollowShards(index2, 1, indexNodeA, indexNodeB);
        } else {
            refresh(index2);
        }

        // we need to wait until we have received shard size updates in the search metrics service
        waitUntil(() -> {
            ConcurrentMap<Index, SearchMetricsService.IndexProperties> indices = searchMetricsService.getIndices();
            boolean bothInteractiveSizePresent = true;
            for (Index i : indices.keySet()) {
                SearchMetricsService.ShardMetrics shardMetric = searchMetricsService.getShardMetrics().get(new ShardId(i, 0));
                if (shardMetric.shardSize.interactiveSizeInBytes() == 0) {
                    bothInteractiveSizePresent = false;
                }
            }
            return bothInteractiveSizePresent;
        }, 2, TimeUnit.SECONDS);

        assertEquals(1, getNumberOfReplicas(index1));
        assertEquals(1, getNumberOfReplicas(index2));

        // switch on relica autoscaling and set SP to 220, which should allow index1 to get two replicas
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder()
                            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 220)
                            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
                            .build()
                    )
                )
                .get()
        );
        // scaling up should happen almost immediately
        waitUntil(() -> getNumberOfReplicas(index1) == 2, 1, TimeUnit.SECONDS);
        assertEquals(1, getNumberOfReplicas(index2));
        assertEquals(2, getNumberOfReplicas(index1));

        // indexing into index2 so that his index now has roughly 2/3 size of total interactive size
        // index1 has 200 docs, index 2 already 100, so we need another 300
        indexDocumentsWithTimestamp(
            index2,
            300,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        if (randomBoolean() && hollowShards) {
            flushAndRefresh(index2);
            hollowShards(index2, 1, indexNodeB, indexNodeA);
        } else {
            refresh(index2);
        }

        // scaling up index2 should happen almost immediately, but we wait 1sec to be sure we catch at least one update interval
        waitUntil(() -> getNumberOfReplicas(index2) == 2, 1, TimeUnit.SECONDS);
        assertEquals(2, getNumberOfReplicas(index2));
        // index1 should still have 2 replicas, it needs 6*500ms for the change to stabiliza
        assertEquals(2, getNumberOfReplicas(index1));

        waitUntil(() -> getNumberOfReplicas(index1) == 1, 4, TimeUnit.SECONDS);
        assertEquals(1, getNumberOfReplicas(index1));
    }

    public void testDisablingReplicasScalesDown() throws Exception {
        // start in a state with an index scaled to two replicas
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
            // disable replicas for load balancing as we only scale down completely (what this test expects) when both replicas scaler
            // systems (instant failover and load balancing) are disabled
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), false)
            .build();
        startMasterOnlyNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);

        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        // new documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            100,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName);
        assertEquals(1, clusterService.state().metadata().getProject().index(indexName).getNumberOfReplicas());
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 250).build()
                    )
                )
                .get()
        );
        waitUntil(() -> clusterService.state().metadata().getProject().index(indexName).getNumberOfReplicas() == 2, 2, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().getProject().index(indexName).getNumberOfReplicas());

        // now disable feature
        setFeatureFlag(false);
        waitUntil(() -> clusterService.state().metadata().getProject().index(indexName).getNumberOfReplicas() == 1, 2, TimeUnit.SECONDS);
        assertEquals(1, clusterService.state().metadata().getProject().index(indexName).getNumberOfReplicas());
    }

    public void testMultipleDatastreamsRanking() throws Exception {
        // setup with auto replica selection disabled
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 205)
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false)
            .build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);

        var cs = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        var sms = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        putComposableIndexTemplate(
            "my-template",
            List.of("logs-*"),
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        final String dataStream1 = "logs-es1";
        setupDataStream(dataStream1);
        final String dataStream2 = "logs-es2";
        setupDataStream(dataStream2);
        ensureGreen();

        // ensure we see stats for all 4 backing indices in SearchMetricsService and that we they all have interactive data
        assertTrue(waitUntil(() -> {
            ReplicaRankingContext rankingContext = sms.createRankingContext();
            return rankingContext.indices().size() == 4
                && rankingContext.properties().stream().filter(i -> i.interactiveSize() == 0).toList().isEmpty();
        }));

        setFeatureFlag(true);
        waitUntil(
            () -> cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream1, 2)).getNumberOfReplicas() == 2
                && cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream2, 2)).getNumberOfReplicas() == 2,
            2,
            TimeUnit.SECONDS
        );
        for (String datastream : new String[] { dataStream1, dataStream2 }) {
            assertEquals(1, cs.state().metadata().getProject().index(getDefaultBackingIndexName(datastream, 1)).getNumberOfReplicas());
            assertEquals(2, cs.state().metadata().getProject().index(getDefaultBackingIndexName(datastream, 2)).getNumberOfReplicas());
        }

        // add third data stream, again with 100 docs in first generation, 100 in current write index after rollover
        final String dataStream3 = "logs-es3";
        setupDataStream(dataStream3);
        verifyDocs("logs-es3", 400, 1, 2);
        waitUntil(
            () -> cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream3, 2)).getNumberOfReplicas() == 2
                && cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream3, 1)).getNumberOfReplicas() == 2,
            2,
            TimeUnit.SECONDS
        );
        // all write indices should have 2 replicas now
        for (String datastream : new String[] { dataStream1, dataStream2, dataStream3 }) {
            assertEquals(2, cs.state().metadata().getProject().index(getDefaultBackingIndexName(datastream, 2)).getNumberOfReplicas());
        }
        // only the youngest (last) backing index (logs-es3) should get two replicas, the other two stay at 1
        assertEquals(1, cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream1, 1)).getNumberOfReplicas());
        assertEquals(1, cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream2, 1)).getNumberOfReplicas());
        assertEquals(2, cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream3, 1)).getNumberOfReplicas());

        // test that data stream with non-interactive data doesn’t get promoted to 2 replicas
        final String dataStream4 = "logs-es4";
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream4);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
        indexDocsIntoDatastream(
            dataStream4,
            200,
            System.currentTimeMillis() - DEFAULT_BOOST_WINDOW - 2 * ONE_DAY,
            System.currentTimeMillis() - DEFAULT_BOOST_WINDOW - ONE_DAY
        );
        refresh(dataStream4);
        // write index should stay at 1 replica because it doesn't contain interactive data
        assertEquals(1, cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream4, 1)).getNumberOfReplicas());

        // add more data to ds4 write index after rolling over, but now inside boost window
        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStream4, null)).get());
        indexDocsIntoDatastream(dataStream4, 200, System.currentTimeMillis() - DEFAULT_BOOST_WINDOW + ONE_DAY, System.currentTimeMillis());
        refresh(dataStream4);
        // this should scale up ds4 write index, ds3 backing index should get demoted to one replica after some time
        waitUntil(
            () -> cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream4, 2)).getNumberOfReplicas() == 2
                && cs.state().metadata().getProject().index(getDefaultBackingIndexName(dataStream3, 1)).getNumberOfReplicas() == 1,
            2,
            TimeUnit.SECONDS
        );
        for (String datastream : new String[] { dataStream1, dataStream2, dataStream3, dataStream4 }) {
            assertEquals(1, cs.state().metadata().getProject().index(getDefaultBackingIndexName(datastream, 1)).getNumberOfReplicas());
            assertEquals(2, cs.state().metadata().getProject().index(getDefaultBackingIndexName(datastream, 2)).getNumberOfReplicas());
        }

        // check that adding a regular index regardless of its small size gets it scaled to 2
        var regularIndex = "index1";
        createIndex(regularIndex, indexSettings(1, 1).build());

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            regularIndex,
            10,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(regularIndex);
        waitUntil(() -> cs.state().metadata().getProject().index(regularIndex).getNumberOfReplicas() == 2, 2, TimeUnit.SECONDS);
        assertEquals(2, cs.state().metadata().getProject().index(regularIndex).getNumberOfReplicas());
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasUpdaterService:DEBUG,"
            + "org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasLoadBalancingScaler:DEBUG",
        reason = "new feature debugging"
    )
    public void testReplicasLoadBalancingScalesUpWithHighSearchLoad() throws Exception {
        // This test does a few things, testing the replicas for load balancing and the integration with instant failover as follows:
        // starts 4 search nodes, enables instant failover (scales to 2 replicas),
        // then enables load balancing (scales searched index to 4 replicas based on traffic). Stops one search node and
        // verifies cluster goes YELLOW until desired topology is updated to 3 nodes. Validates interactions between
        // instant failover and load balancing settings, and that SPmin below 100 forces 1 replica regardless of settings.
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 250)
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false)
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), false)
            .build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        startSearchNode(settings);
        startSearchNode(settings);
        String aSearchNode = startSearchNode(settings);

        String indexWithSearchTraffic = "searched-index";
        String idleIndex = "idle-index";
        createIndex(indexWithSearchTraffic, indexSettings(1, 1).put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false).build());
        createIndex(idleIndex, indexSettings(1, 1).put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false).build());
        ensureGreen();

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(indexWithSearchTraffic, 200, boostWindow + ONE_DAY, now);
        indexDocumentsWithTimestamp(idleIndex, 200, boostWindow + ONE_DAY, now);

        // replicas for load balancing doesn't work without desired topology
        DesiredTopologyContext desiredTopologyContext = internalCluster().getCurrentMasterNodeInstance(DesiredTopologyContext.class);
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(new TierTopology(4, "8G", 32.5f, 0.5f, 1.0f), new TierTopology(1, "8G", 32.5f, 0.5f, 1.0f))
        );

        // make some search traffic for the searched index
        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        // instant failover at SPmin 250 should give 2 replicas to both indices, and no more
        updateClusterSettings(Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true));
        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        assertBusy(() -> {
            ClusterState state = clusterService.state();
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(2));
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(2));
        });

        logger.info("-> Enabling replicas for load balancing");
        updateClusterSettings(Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), true));
        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        assertBusy(() -> {
            ClusterState state = clusterService.state();
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(4));
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(2));
        });

        // keep the search load high for one index
        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        // stop a search node, without updating desired topology, we should remain in YELLOW until we receive the updated topology (with 3
        // search nodes)
        internalCluster().stopNode(aSearchNode);
        // keep the search load high for one index
        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        ensureYellow(indexWithSearchTraffic);

        // keep the search load high for one index
        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        // desired topology now reflects 3 search nodes so we should go to green, with 3 replicas for the searched index
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(new TierTopology(3, "8G", 32.5f, 0.5f, 1.0f), new TierTopology(1, "8G", 32.5f, 0.5f, 1.0f))
        );
        assertBusy(
            () -> assertThat(
                clusterService.state().metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(),
                is(3)
            )
        );

        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        // disabling replicas for instant failover should reduce the number of replicas for the idle index (to 1)
        // but the replicas for load balancing scaler will keep the searched index to max (3)
        updateClusterSettings(Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false));
        assertBusy(() -> {
            ClusterState state = clusterService.state();
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(3));
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(1));
        });
        ensureGreen();

        // re-enabling replicas for instant failover, but disabling replicas for load balancing should
        // bring both indices to 2 replicas (SPmin 250)
        updateClusterSettings(
            Settings.builder()
                .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
                .put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), false)
        );
        assertBusy(() -> {
            ClusterState state = clusterService.state();
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(2));
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(2));
        });
        ensureGreen();

        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        // updating SPmin to below 100 will see instant failover recommend 1 replica for everything
        // but load balancing will recommend more for the index with search traffic, so we expect this
        // searched index to be at 3 replicas (max possible on 3 seach nodes)
        updateClusterSettings(
            Settings.builder()
                .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
                .put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), true)
                .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 45)
        );
        assertBusy(() -> {
            ClusterState state = clusterService.state();
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(3));
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(1));
        });

        // disabling replicas for load balancing leaves only instant failover running which at SPmin < 100 recommends 1 replica for
        // everything
        updateClusterSettings(Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), false));
        assertBusy(() -> {
            ClusterState state = clusterService.state();
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(1));
            assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(1));
        });
    }

    public void testReplicasForLoadBalancingWorksAtLowSPs() throws Exception {
        int lowSpMin = randomIntBetween(5, 45);
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), lowSpMin)
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false)
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), false)
            .build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        startSearchNode(settings);
        startSearchNode(settings);

        String indexWithSearchTraffic = "searched-index";
        String idleIndex = "idle-index";
        createIndex(indexWithSearchTraffic, indexSettings(1, 1).put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false).build());
        createIndex(idleIndex, indexSettings(1, 1).put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false).build());
        ensureGreen();

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(indexWithSearchTraffic, 200, boostWindow + ONE_DAY, now);
        indexDocumentsWithTimestamp(idleIndex, 200, boostWindow + ONE_DAY, now);

        DesiredTopologyContext desiredTopologyContext = internalCluster().getCurrentMasterNodeInstance(DesiredTopologyContext.class);
        desiredTopologyContext.updateDesiredClusterTopology(
            new DesiredClusterTopology(new TierTopology(3, "8G", 32.5f, 0.5f, 1.0f), new TierTopology(1, "8G", 32.5f, 0.5f, 1.0f))
        );

        // make some search traffic for the searched index
        createSearchTraffic(indexWithSearchTraffic, boostWindow);
        // instant failover at low SPmin should give 1 replica to both indices, so no change here
        updateClusterSettings(Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true));
        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        ClusterState state = clusterService.state();
        assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(1));
        assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(1));

        // enabling replicas load balancing shouldn't do much as there's no index search load yet (i.e. it'll just recommend 1 replica for
        // both indices)
        updateClusterSettings(Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_LOAD_BALANCING.getKey(), true));

        state = clusterService.state();
        assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(), is(1));
        assertThat(state.metadata().getProject(ProjectId.DEFAULT).index(idleIndex).getNumberOfReplicas(), is(1));

        // one index gets some search traffic, so should get more replicas
        createSearchTraffic(indexWithSearchTraffic, boostWindow);

        assertBusy(
            () -> assertThat(
                clusterService.state().metadata().getProject(ProjectId.DEFAULT).index(indexWithSearchTraffic).getNumberOfReplicas(),
                is(3)
            )
        );
    }

    private static void createSearchTraffic(String indexWithSearchTraffic, long boostWindow) {
        for (int i = 0; i < 100; i++) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(new RangeQueryBuilder("@timestamp").from(randomBoolean() ? boostWindow : boostWindow + ONE_DAY));
            SearchResponse searchResponse = client().prepareSearch(indexWithSearchTraffic)
                .setQuery(boolQueryBuilder)
                .setRequestCache(false)
                .get();
            try {
                assertNoFailures(searchResponse);
            } finally {
                searchResponse.decRef();
            }
        }
    }

    private static void verifyDocs(String dataStream, long expectedNumHits, long minGeneration, long maxGeneration) {
        List<String> expectedIndices = new ArrayList<>();
        for (long k = minGeneration; k <= maxGeneration; k++) {
            expectedIndices.add(getDefaultBackingIndexName(dataStream, k));
        }
        assertResponse(prepareSearch(dataStream).setSize((int) expectedNumHits), resp -> {
            assertThat(resp.getHits().getTotalHits().value(), equalTo(expectedNumHits));
            Arrays.stream(resp.getHits().getHits()).forEach(hit -> assertTrue(expectedIndices.contains(hit.getIndex())));
        });
    }

    private void setupDataStream(String dataStreamName) throws InterruptedException, ExecutionException {
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocsIntoDatastream(
            dataStreamName,
            200,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get());
        indexDocsIntoDatastream(
            dataStreamName,
            200,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(dataStreamName);
    }

    private static void indexDocsIntoDatastream(String dataStream, int numDocs, long minTimestamp, long maxTimestamp) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(randomLongBetween(minTimestamp, maxTimestamp));
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    private static void putComposableIndexTemplate(String id, List<String> patterns, @Nullable Settings settings) {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().settings(settings))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    private static void setFeatureFlag(boolean enabled) throws ExecutionException, InterruptedException {
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), enabled).build()
                    )
                )
                .get()
        );
    }

    private void indexDocumentsWithTimestamp(String indexName, int numDocs, long minTimestamp, long maxTimestamp) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).source(DataStream.TIMESTAMP_FIELD_NAME, randomLongBetween(minTimestamp, maxTimestamp))
            );
        }
        assertNoFailures(bulkRequest.get());
    }
}
