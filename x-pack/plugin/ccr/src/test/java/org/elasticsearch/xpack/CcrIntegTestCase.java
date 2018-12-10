/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.DocIdSeqNoAndTerm;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.LocalStateCcr;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngine;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class CcrIntegTestCase extends ESTestCase {

    private static ClusterGroup clusterGroup;

    @Before
    public final void startClusters() throws Exception {
        if (clusterGroup != null && reuseClusters()) {
            clusterGroup.leaderCluster.ensureAtMostNumDataNodes(numberOfNodesPerCluster());
            clusterGroup.followerCluster.ensureAtMostNumDataNodes(numberOfNodesPerCluster());
            return;
        }

        stopClusters();
        NodeConfigurationSource nodeConfigurationSource = createNodeConfigurationSource();
        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(ESIntegTestCase.TestSeedPlugin.class,
            TestZenDiscovery.TestPlugin.class, MockHttpTransport.TestPlugin.class, getTestTransportPlugin());

        InternalTestCluster leaderCluster = new InternalTestCluster(randomLong(), createTempDir(), true, true, numberOfNodesPerCluster(),
            numberOfNodesPerCluster(), UUIDs.randomBase64UUID(random()), nodeConfigurationSource, 0, "leader", mockPlugins,
            Function.identity());
        InternalTestCluster followerCluster = new InternalTestCluster(randomLong(), createTempDir(), true, true, numberOfNodesPerCluster(),
            numberOfNodesPerCluster(), UUIDs.randomBase64UUID(random()), nodeConfigurationSource, 0, "follower", mockPlugins,
            Function.identity());
        clusterGroup = new ClusterGroup(leaderCluster, followerCluster);

        leaderCluster.beforeTest(random(), 0.0D);
        leaderCluster.ensureAtLeastNumDataNodes(numberOfNodesPerCluster());
        followerCluster.beforeTest(random(), 0.0D);
        followerCluster.ensureAtLeastNumDataNodes(numberOfNodesPerCluster());

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        String address = leaderCluster.getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        updateSettingsRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    /**
     * Follower indices don't get all the settings from leader, for example 'index.unassigned.node_left.delayed_timeout'
     * is not replicated and if tests kill nodes, we have to wait 60s by default...
     */
    protected void disableDelayedAllocation(String index) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index);
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
        updateSettingsRequest.settings(settingsBuilder);
        assertAcked(followerClient().admin().indices().updateSettings(updateSettingsRequest).actionGet());
    }

    @After
    public void afterTest() throws Exception {
        ensureEmptyWriteBuffers();
        String masterNode = clusterGroup.followerCluster.getMasterName();
        ClusterService clusterService = clusterGroup.followerCluster.getInstance(ClusterService.class, masterNode);
        removeCCRRelatedMetadataFromClusterState(clusterService);

        try {
            clusterGroup.leaderCluster.beforeIndexDeletion();
            clusterGroup.leaderCluster.assertSeqNos();
            clusterGroup.leaderCluster.assertSameDocIdsOnShards();
            clusterGroup.leaderCluster.assertConsistentHistoryBetweenTranslogAndLuceneIndex();

            clusterGroup.followerCluster.beforeIndexDeletion();
            clusterGroup.followerCluster.assertSeqNos();
            clusterGroup.followerCluster.assertSameDocIdsOnShards();
            clusterGroup.followerCluster.assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        } finally {
            clusterGroup.leaderCluster.wipe(Collections.emptySet());
            clusterGroup.followerCluster.wipe(Collections.emptySet());
        }
    }

    private NodeConfigurationSource createNodeConfigurationSource() {
        Settings.Builder builder = Settings.builder();
        builder.put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), Integer.MAX_VALUE);
        // Default the watermarks to absurdly low to prevent the tests
        // from failing on nodes without enough disk space
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b");
        builder.put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getKey(), "2048/1m");
        // wait short time for other active shards before actually deleting, default 30s not needed in tests
        builder.put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS));
        builder.putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()); // empty list disables a port scan for other nodes
        builder.putList(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "file");
        builder.put(TestZenDiscovery.USE_ZEN2.getKey(), false); // some tests do full cluster restarts
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        builder.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        builder.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return builder.build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return Arrays.asList(LocalStateCcr.class, CommonAnalysisPlugin.class);
            }

            @Override
            public Settings transportClientSettings() {
                return super.transportClientSettings();
            }

            @Override
            public Collection<Class<? extends Plugin>> transportClientPlugins() {
                return Arrays.asList(LocalStateCcr.class, getTestTransportPlugin());
            }
        };
    }

    @AfterClass
    public static void stopClusters() throws IOException {
        IOUtils.close(clusterGroup);
        clusterGroup = null;
    }

    protected int numberOfNodesPerCluster() {
        return 2;
    }

    protected boolean reuseClusters() {
        return true;
    }

    protected final Client leaderClient() {
        return clusterGroup.leaderCluster.client();
    }

    protected final Client followerClient() {
        return clusterGroup.followerCluster.client();
    }

    protected final InternalTestCluster getLeaderCluster() {
        return clusterGroup.leaderCluster;
    }

    protected final InternalTestCluster getFollowerCluster() {
        return clusterGroup.followerCluster;
    }

    protected final ClusterHealthStatus ensureLeaderYellow(String... indices) {
        return ensureColor(clusterGroup.leaderCluster, ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), false, indices);
    }

    protected final ClusterHealthStatus ensureLeaderGreen(String... indices) {
        logger.info("ensure green leader indices {}", Arrays.toString(indices));
        return ensureColor(clusterGroup.leaderCluster, ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30), false, indices);
    }

    protected final ClusterHealthStatus ensureFollowerGreen(String... indices) {
        logger.info("ensure green follower indices {}", Arrays.toString(indices));
        return ensureColor(clusterGroup.followerCluster, ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30), false, indices);
    }

    private ClusterHealthStatus ensureColor(TestCluster testCluster,
                                            ClusterHealthStatus clusterHealthStatus,
                                            TimeValue timeout,
                                            boolean waitForNoInitializingShards,
                                            String... indices) {
        String color = clusterHealthStatus.name().toLowerCase(Locale.ROOT);
        String method = "ensure" + Strings.capitalize(color);

        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest(indices)
            .timeout(timeout)
            .waitForStatus(clusterHealthStatus)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(waitForNoInitializingShards)
            .waitForNodes(Integer.toString(testCluster.size()));

        ClusterHealthResponse actionGet = testCluster.client().admin().cluster().health(healthRequest).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("{} timed out, cluster state:\n{}\n{}",
                method,
                testCluster.client().admin().cluster().prepareState().get().getState(),
                testCluster.client().admin().cluster().preparePendingClusterTasks().get());
            fail("timed out waiting for " + color + " state");
        }
        assertThat("Expected at least " + clusterHealthStatus + " but got " + actionGet.getStatus(),
            actionGet.getStatus().value(), lessThanOrEqualTo(clusterHealthStatus.value()));
        logger.debug("indices {} are {}", indices.length == 0 ? "[_all]" : indices, color);
        return actionGet.getStatus();
    }

    protected final Index resolveLeaderIndex(String index) {
        GetIndexResponse getIndexResponse = leaderClient().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetaData.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected final Index resolveFollowerIndex(String index) {
        GetIndexResponse getIndexResponse = followerClient().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetaData.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected final RefreshResponse refresh(Client client, String... indices) {
        RefreshResponse actionGet = client.admin().indices().prepareRefresh(indices).execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected void ensureEmptyWriteBuffers() throws Exception {
        assertBusy(() -> {
            FollowStatsAction.StatsResponses statsResponses =
                leaderClient().execute(FollowStatsAction.INSTANCE, new FollowStatsAction.StatsRequest()).actionGet();
            for (FollowStatsAction.StatsResponse statsResponse : statsResponses.getStatsResponses()) {
                ShardFollowNodeTaskStatus status = statsResponse.status();
                assertThat(status.writeBufferOperationCount(), equalTo(0));
                assertThat(status.writeBufferSizeInBytes(), equalTo(0L));
            }
        });
    }

    protected void pauseFollow(String... indices) throws Exception {
        for (String index : indices) {
            final PauseFollowAction.Request unfollowRequest = new PauseFollowAction.Request(index);
            followerClient().execute(PauseFollowAction.INSTANCE, unfollowRequest).get();
        }
        ensureNoCcrTasks();
    }

    protected void ensureNoCcrTasks() throws Exception {
        assertBusy(() -> {
            final ClusterState clusterState = followerClient().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(tasks.tasks(), empty());

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            ListTasksResponse listTasksResponse = followerClient().admin().cluster().listTasks(listTasksRequest).get();
            int numNodeTasks = 0;
            for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                if (taskInfo.getAction().startsWith(ListTasksAction.NAME) == false) {
                    numNodeTasks++;
                }
            }
            assertThat(numNodeTasks, equalTo(0));
        }, 30, TimeUnit.SECONDS);
    }

    protected String getIndexSettings(final int numberOfShards, final int numberOfReplicas,
                                    final Map<String, String> additionalIndexSettings) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
                    builder.field("index.number_of_shards", numberOfShards);
                    builder.field("index.number_of_replicas", numberOfReplicas);
                    for (final Map.Entry<String, String> additionalSetting : additionalIndexSettings.entrySet()) {
                        builder.field(additionalSetting.getKey(), additionalSetting.getValue());
                    }
                }
                builder.endObject();
                builder.startObject("mappings");
                {
                    builder.startObject("doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("f");
                            {
                                builder.field("type", "integer");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

    public static PutFollowAction.Request putFollow(String leaderIndex, String followerIndex) {
        PutFollowAction.Request request = new PutFollowAction.Request();
        request.setRemoteCluster("leader_cluster");
        request.setLeaderIndex(leaderIndex);
        request.setFollowRequest(resumeFollow(followerIndex));
        return request;
    }

    public static ResumeFollowAction.Request resumeFollow(String followerIndex) {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setFollowerIndex(followerIndex);
        request.setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.setReadPollTimeout(TimeValue.timeValueMillis(10));
        return request;
    }

    /**
     * This asserts the index is fully replicated from the leader index to the follower index. It first verifies that the seq_no_stats
     * on the follower equal the leader's; then verifies the existing pairs of (docId, seqNo) on the follower also equal the leader.
     */
    protected void assertIndexFullyReplicatedToFollower(String leaderIndex, String followerIndex) throws Exception {
        logger.info("--> asserting seq_no_stats between {} and {}", leaderIndex, followerIndex);
        assertBusy(() -> {
            Map<Integer, SeqNoStats> leaderStats = new HashMap<>();
            for (ShardStats shardStat : leaderClient().admin().indices().prepareStats(leaderIndex).clear().get().getShards()) {
                if (shardStat.getSeqNoStats() == null) {
                    throw new AssertionError("leader seq_no_stats is not available [" + Strings.toString(shardStat) + "]");
                }
                leaderStats.put(shardStat.getShardRouting().shardId().id(), shardStat.getSeqNoStats());
            }
            Map<Integer, SeqNoStats> followerStats = new HashMap<>();
            for (ShardStats shardStat : followerClient().admin().indices().prepareStats(followerIndex).clear().get().getShards()) {
                if (shardStat.getSeqNoStats() == null) {
                    throw new AssertionError("follower seq_no_stats is not available [" + Strings.toString(shardStat) + "]");
                }
                followerStats.put(shardStat.getShardRouting().shardId().id(), shardStat.getSeqNoStats());
            }
            assertThat(leaderStats, equalTo(followerStats));
        }, 60, TimeUnit.SECONDS);
        logger.info("--> asserting <<docId,seqNo>> between {} and {}", leaderIndex, followerIndex);
        assertBusy(() -> {
            assertThat(getDocIdAndSeqNos(clusterGroup.leaderCluster, leaderIndex),
                equalTo(getDocIdAndSeqNos(clusterGroup.followerCluster, followerIndex)));
        }, 60, TimeUnit.SECONDS);
    }

    private Map<Integer, List<DocIdSeqNoAndTerm>> getDocIdAndSeqNos(InternalTestCluster cluster, String index) throws IOException {
        final ClusterState state = cluster.client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> shardRoutings = state.routingTable().allShards(index);
        Randomness.shuffle(shardRoutings);
        final Map<Integer, List<DocIdSeqNoAndTerm>> docs = new HashMap<>();
        for (ShardRouting shardRouting : shardRoutings) {
            if (shardRouting == null || shardRouting.assignedToNode() == false || docs.containsKey(shardRouting.shardId().id())) {
                continue;
            }
            IndexShard indexShard = cluster.getInstance(IndicesService.class, state.nodes().get(shardRouting.currentNodeId()).getName())
                .indexServiceSafe(shardRouting.index()).getShard(shardRouting.id());
            docs.put(shardRouting.shardId().id(), IndexShardTestCase.getDocIdAndSeqNos(indexShard).stream()
                .map(d -> new DocIdSeqNoAndTerm(d.getId(), d.getSeqNo(), 1L))  // normalize primary term as the follower use its own term
                .collect(Collectors.toList()));
        }
        return docs;
    }

    protected void atLeastDocsIndexed(Client client, String index, long numDocsReplicated) throws InterruptedException {
        logger.info("waiting for at least [{}] documents to be indexed into index [{}]", numDocsReplicated, index);
        awaitBusy(() -> {
            refresh(client, index);
            SearchRequest request = new SearchRequest(index);
            request.source(new SearchSourceBuilder().size(0));
            SearchResponse response = client.search(request).actionGet();
            return response.getHits().getTotalHits().value >= numDocsReplicated;
        }, 60, TimeUnit.SECONDS);
    }

    protected void awaitGlobalCheckpointAtLeast(Client client, ShardId shardId, long minimumGlobalCheckpoint) throws Exception {
        logger.info("waiting for the global checkpoint on [{}] at least [{}]", shardId, minimumGlobalCheckpoint);
        assertBusy(() -> {
            ShardStats stats = client.admin().indices().prepareStats(shardId.getIndexName()).clear().get()
                .asMap().entrySet().stream().filter(e -> e.getKey().shardId().equals(shardId))
                .map(Map.Entry::getValue).findFirst().orElse(null);
            if (stats == null || stats.getSeqNoStats() == null) {
                throw new AssertionError("seq_no_stats for shard [" + shardId + "] is not found"); // causes assertBusy to retry
            }
            assertThat(Strings.toString(stats.getSeqNoStats()),
                stats.getSeqNoStats().getGlobalCheckpoint(), greaterThanOrEqualTo(minimumGlobalCheckpoint));
        }, 60, TimeUnit.SECONDS);
    }

    protected void assertMaxSeqNoOfUpdatesIsTransferred(Index leaderIndex, Index followerIndex, int numberOfShards) throws Exception {
        assertBusy(() -> {
            long[] msuOnLeader = new long[numberOfShards];
            for (int i = 0; i < msuOnLeader.length; i++) {
                msuOnLeader[i] = SequenceNumbers.UNASSIGNED_SEQ_NO;
            }
            Set<String> leaderNodes = getLeaderCluster().nodesInclude(leaderIndex.getName());
            for (String leaderNode : leaderNodes) {
                IndicesService indicesService = getLeaderCluster().getInstance(IndicesService.class, leaderNode);
                for (int i = 0; i < numberOfShards; i++) {
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(leaderIndex, i));
                    if (shard != null) {
                        try {
                            msuOnLeader[i] = SequenceNumbers.max(msuOnLeader[i], shard.getMaxSeqNoOfUpdatesOrDeletes());
                        } catch (AlreadyClosedException ignored) {
                            return;
                        }
                    }
                }
            }

            Set<String> followerNodes = getFollowerCluster().nodesInclude(followerIndex.getName());
            for (String followerNode : followerNodes) {
                IndicesService indicesService = getFollowerCluster().getInstance(IndicesService.class, followerNode);
                for (int i = 0; i < numberOfShards; i++) {
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(leaderIndex, i));
                    if (shard != null) {
                        try {
                            assertThat(shard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(msuOnLeader[i]));
                        } catch (AlreadyClosedException ignored) {

                        }
                    }
                }
            }
        });
    }

    protected void assertTotalNumberOfOptimizedIndexing(Index followerIndex, int numberOfShards, long expectedTotal) throws Exception {
        assertBusy(() -> {
            long[] numOfOptimizedOps = new long[numberOfShards];
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                for (String node : getFollowerCluster().nodesInclude(followerIndex.getName())) {
                    IndicesService indicesService = getFollowerCluster().getInstance(IndicesService.class, node);
                    IndexShard shard = indicesService.getShardOrNull(new ShardId(followerIndex, shardId));
                    if (shard != null && shard.routingEntry().primary()) {
                        try {
                            FollowingEngine engine = ((FollowingEngine) IndexShardTestCase.getEngine(shard));
                            numOfOptimizedOps[shardId] = engine.getNumberOfOptimizedIndexing();
                        } catch (AlreadyClosedException e) {
                            throw new AssertionError(e); // causes assertBusy to retry
                        }
                    }
                }
            }
            assertThat(Arrays.stream(numOfOptimizedOps).sum(), equalTo(expectedTotal));
        });
    }

    static void removeCCRRelatedMetadataFromClusterState(ClusterService clusterService) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("remove-ccr-related-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder newState = ClusterState.builder(currentState);
                newState.metaData(MetaData.builder(currentState.getMetaData())
                    .removeCustom(AutoFollowMetadata.TYPE)
                    .removeCustom(PersistentTasksCustomMetaData.TYPE)
                    .build());
                return newState.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });
        latch.await();
    }

    static class ClusterGroup implements Closeable {

        final InternalTestCluster leaderCluster;
        final InternalTestCluster followerCluster;

        ClusterGroup(InternalTestCluster leaderCluster, InternalTestCluster followerCluster) {
            this.leaderCluster = leaderCluster;
            this.followerCluster = followerCluster;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(leaderCluster, followerCluster);
        }
    }
}
