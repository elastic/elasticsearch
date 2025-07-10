/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack;

import org.apache.logging.log4j.Level;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.CloseableTestClusterWrapper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.LocalStateCcr;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;
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
import java.util.stream.Stream;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.snapshots.RestoreService.restoreInProgress;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class CcrIntegTestCase extends ESTestCase {

    private static ClusterGroup clusterGroup;

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }

    protected Settings leaderClusterSettings() {
        return Settings.EMPTY;
    }

    protected Settings followerClusterSettings() {
        final Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(RemoteConnectionStrategy.REMOTE_MAX_PENDING_CONNECTION_LISTENERS.getKey(), randomIntBetween(1, 100));
        }
        return builder.build();
    }

    @Before
    public final void startClusters() throws Exception {
        if (clusterGroup != null && reuseClusters()) {
            clusterGroup.leaderCluster.ensureAtMostNumDataNodes(numberOfNodesPerCluster());
            clusterGroup.followerCluster.ensureAtMostNumDataNodes(numberOfNodesPerCluster());
            setupMasterNodeRequestsValidatorOnFollowerCluster();
            return;
        }

        stopClusters();
        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(
            ESIntegTestCase.TestSeedPlugin.class,
            MockHttpTransport.TestPlugin.class,
            MockTransportService.TestPlugin.class,
            InternalSettingsPlugin.class,
            getTestTransportPlugin()
        );

        InternalTestCluster leaderCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            true,
            true,
            numberOfNodesPerCluster(),
            numberOfNodesPerCluster(),
            "leader_cluster",
            createNodeConfigurationSource(null, true),
            0,
            "leader",
            mockPlugins,
            Function.identity()
        );
        leaderCluster.beforeTest(random());
        leaderCluster.ensureAtLeastNumDataNodes(numberOfNodesPerCluster());
        assertBusy(() -> {
            ClusterService clusterService = leaderCluster.getInstance(ClusterService.class);
            assertNotNull(clusterService.state().metadata().custom(LicensesMetadata.TYPE));
        }, 60, TimeUnit.SECONDS);

        String address = leaderCluster.getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        InternalTestCluster followerCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            true,
            true,
            numberOfNodesPerCluster(),
            numberOfNodesPerCluster(),
            "follower_cluster",
            createNodeConfigurationSource(address, false),
            0,
            "follower",
            mockPlugins,
            Function.identity()
        );
        clusterGroup = new ClusterGroup(leaderCluster, followerCluster);

        followerCluster.beforeTest(random());
        followerCluster.ensureAtLeastNumDataNodes(numberOfNodesPerCluster());
        assertBusy(() -> {
            ClusterService clusterService = followerCluster.getInstance(ClusterService.class);
            assertNotNull(clusterService.state().metadata().custom(LicensesMetadata.TYPE));
        }, 60, TimeUnit.SECONDS);
        setupMasterNodeRequestsValidatorOnFollowerCluster();
    }

    protected void setupMasterNodeRequestsValidatorOnFollowerCluster() {
        final InternalTestCluster followerCluster = clusterGroup.followerCluster;
        for (String nodeName : followerCluster.getNodeNames()) {
            MockTransportService transportService = (MockTransportService) followerCluster.getInstance(TransportService.class, nodeName);
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (isCcrAdminRequest(request) == false && request instanceof AcknowledgedRequest<?> acknowledgedRequest) {
                    final TimeValue masterTimeout = acknowledgedRequest.masterNodeTimeout();
                    if (masterTimeout == null || masterTimeout.nanos() != TimeValue.MAX_VALUE.nanos()) {
                        throw new AssertionError("time out of a master request [" + request + "] on the follower is not set to unbounded");
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
    }

    protected void removeMasterNodeRequestsValidatorOnFollowerCluster() {
        final InternalTestCluster followerCluster = clusterGroup.followerCluster;
        for (String nodeName : followerCluster.getNodeNames()) {
            MockTransportService transportService = (MockTransportService) getFollowerCluster().getInstance(
                TransportService.class,
                nodeName
            );
            transportService.clearAllRules();
        }
    }

    private static boolean isCcrAdminRequest(TransportRequest request) {
        return request instanceof PutFollowAction.Request
            || request instanceof ResumeFollowAction.Request
            || request instanceof PauseFollowAction.Request
            || request instanceof UnfollowAction.Request
            || request instanceof ForgetFollowerAction.Request
            || request instanceof PutAutoFollowPatternAction.Request
            || request instanceof ActivateAutoFollowPatternAction.Request
            || request instanceof DeleteAutoFollowPatternAction.Request;
    }

    /**
     * Follower indices don't get all the settings from leader, for example 'index.unassigned.node_left.delayed_timeout'
     * is not replicated and if tests kill nodes, we have to wait 60s by default...
     */
    protected void disableDelayedAllocation(String index) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).masterNodeTimeout(TimeValue.MAX_VALUE);
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
        updateSettingsRequest.settings(settingsBuilder);
        assertAcked(followerClient().admin().indices().updateSettings(updateSettingsRequest).actionGet());
    }

    @After
    public void afterTest() throws Exception {
        ensureEmptyWriteBuffers();
        removeMasterNodeRequestsValidatorOnFollowerCluster();
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

        clusterGroup.leaderCluster.assertAfterTest();
        clusterGroup.followerCluster.assertAfterTest();
    }

    private NodeConfigurationSource createNodeConfigurationSource(final String leaderSeedAddress, final boolean leaderCluster) {
        Settings.Builder builder = Settings.builder();
        // Default the watermarks to absurdly low to prevent the tests
        // from failing on nodes without enough disk space
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b");
        // wait short time for other active shards before actually deleting, default 30s not needed in tests
        builder.put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS));
        builder.putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()); // empty list disables a port scan for other nodes
        builder.putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file");
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        builder.put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        // Let cluster state api return quickly in order to speed up auto follow tests:
        builder.put(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT.getKey(), TimeValue.timeValueMillis(100));
        if (leaderCluster) {
            builder.put(leaderClusterSettings());
        } else {
            builder.put(followerClusterSettings());
        }
        if (configureRemoteClusterViaNodeSettings() && leaderSeedAddress != null) {
            builder.put("cluster.remote.leader_cluster.seeds", leaderSeedAddress);
        }
        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
                return builder.build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return Stream.concat(
                    Stream.of(LocalStateCcr.class, CommonAnalysisPlugin.class),
                    CcrIntegTestCase.this.nodePlugins().stream()
                ).collect(Collectors.toList());
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

    protected boolean configureRemoteClusterViaNodeSettings() {
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
        return ensureFollowerGreen(false, indices);
    }

    protected final ClusterHealthStatus ensureFollowerGreen(boolean waitForNoInitializingShards, String... indices) {
        logger.info("ensure green follower indices {}", Arrays.toString(indices));
        return ensureColor(
            clusterGroup.followerCluster,
            ClusterHealthStatus.GREEN,
            TimeValue.timeValueSeconds(60),
            waitForNoInitializingShards,
            indices
        );
    }

    private ClusterHealthStatus ensureColor(
        TestCluster testCluster,
        ClusterHealthStatus clusterHealthStatus,
        TimeValue timeout,
        boolean waitForNoInitializingShards,
        String... indices
    ) {
        String color = clusterHealthStatus.name().toLowerCase(Locale.ROOT);
        String method = "ensure" + Strings.capitalize(color);

        ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, indices).masterNodeTimeout(timeout)
            .timeout(timeout)
            .waitForStatus(clusterHealthStatus)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(waitForNoInitializingShards)
            .waitForNodes(Integer.toString(testCluster.size()));

        ClusterHealthResponse actionGet = testCluster.client().admin().cluster().health(healthRequest).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                """
                    {} timed out:
                    leader cluster state:
                    {}
                    leader cluster tasks:
                    {}
                    follower cluster state:
                    {}
                    follower cluster tasks:
                    {}""",
                method,
                leaderClient().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState(),
                ESIntegTestCase.getClusterPendingTasks(leaderClient()),
                followerClient().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState(),
                ESIntegTestCase.getClusterPendingTasks(followerClient())
            );
            HotThreads.logLocalHotThreads(logger, Level.INFO, "hot threads at timeout", ReferenceDocs.LOGGING);
            fail("timed out waiting for " + color + " state");
        }
        assertThat(
            "Expected at least " + clusterHealthStatus + " but got " + actionGet.getStatus(),
            actionGet.getStatus().value(),
            lessThanOrEqualTo(clusterHealthStatus.value())
        );
        logger.debug("indices {} are {}", indices.length == 0 ? "[_all]" : indices, color);
        return actionGet.getStatus();
    }

    protected final Index resolveLeaderIndex(String index) {
        GetIndexResponse getIndexResponse = leaderClient().admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected final Index resolveFollowerIndex(String index) {
        GetIndexResponse getIndexResponse = followerClient().admin()
            .indices()
            .prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices(index)
            .get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected final BroadcastResponse refresh(Client client, String... indices) {
        BroadcastResponse actionGet = client.admin().indices().prepareRefresh(indices).get();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected void ensureEmptyWriteBuffers() throws Exception {
        assertBusy(() -> {
            FollowStatsAction.StatsResponses statsResponses = leaderClient().execute(
                FollowStatsAction.INSTANCE,
                new FollowStatsAction.StatsRequest()
            ).actionGet();
            for (FollowStatsAction.StatsResponse statsResponse : statsResponses.getStatsResponses()) {
                ShardFollowNodeTaskStatus status = statsResponse.status();
                assertThat(status.writeBufferOperationCount(), equalTo(0));
                assertThat(status.writeBufferSizeInBytes(), equalTo(0L));
            }
        });
    }

    protected void pauseFollow(String... indices) throws Exception {
        for (String index : indices) {
            final PauseFollowAction.Request unfollowRequest = new PauseFollowAction.Request(TEST_REQUEST_TIMEOUT, index);
            assertAcked(followerClient().execute(PauseFollowAction.INSTANCE, unfollowRequest).actionGet());
        }
        ensureNoCcrTasks();
    }

    protected void ensureNoCcrTasks() throws Exception {
        assertBusy(() -> {
            CcrStatsAction.Response statsResponse = followerClient().execute(
                CcrStatsAction.INSTANCE,
                new CcrStatsAction.Request(TEST_REQUEST_TIMEOUT)
            ).actionGet();
            assertThat(
                "Follow stats not empty: " + Strings.toString(statsResponse.getFollowStats()),
                statsResponse.getFollowStats().getStatsResponses(),
                empty()
            );

            final ClusterState clusterState = followerClient().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            PersistentTasksCustomMetadata tasks = clusterState.metadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> ccrTasks = tasks.tasks()
                .stream()
                .filter(t -> t.getTaskName().equals(ShardFollowTask.NAME))
                .toList();
            assertThat(ccrTasks, empty());

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            ListTasksResponse listTasksResponse = followerClient().admin().cluster().listTasks(listTasksRequest).get();
            int numNodeTasks = 0;
            for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                if (taskInfo.action().startsWith(ShardFollowTask.NAME)) {
                    numNodeTasks++;
                }
            }
            assertThat(listTasksResponse.getTasks().toString(), numNodeTasks, equalTo(0));
        }, 30, TimeUnit.SECONDS);
    }

    @Before
    public void setupSourceEnabledOrDisabled() {
        sourceEnabled = randomBoolean();
    }

    protected boolean sourceEnabled;

    protected String getIndexSettings(final int numberOfShards, final int numberOfReplicas) throws IOException {
        return getIndexSettings(numberOfShards, numberOfReplicas, Collections.emptyMap());
    }

    protected String getIndexSettings(
        final int numberOfShards,
        final int numberOfReplicas,
        final Map<String, String> additionalIndexSettings
    ) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
                    builder.field(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s");
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
                        if (sourceEnabled == false) {
                            builder.startObject("_source");
                            builder.field("enabled", false);
                            builder.endObject();
                        }
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
        return putFollow(leaderIndex, followerIndex, ActiveShardCount.ONE);
    }

    public static PutFollowAction.Request putFollow(String leaderIndex, String followerIndex, ActiveShardCount waitForActiveShards) {
        PutFollowAction.Request request = new PutFollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.setRemoteCluster("leader_cluster");
        request.setLeaderIndex(leaderIndex);
        request.setFollowerIndex(followerIndex);
        request.getParameters().setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(10));
        request.getParameters().setMaxReadRequestSize(ByteSizeValue.ofBytes(between(1, 32 * 1024 * 1024)));
        request.getParameters().setMaxReadRequestOperationCount(between(1, 10000));
        request.waitForActiveShards(waitForActiveShards);
        if (randomBoolean()) {
            request.masterNodeTimeout(TimeValue.timeValueSeconds(randomFrom(10, 20, 30)));
        }
        return request;
    }

    public static ResumeFollowAction.Request resumeFollow(String followerIndex) {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request(TEST_REQUEST_TIMEOUT);
        request.setFollowerIndex(followerIndex);
        request.getParameters().setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(10));
        if (randomBoolean()) {
            request.masterNodeTimeout(TimeValue.timeValueSeconds(randomFrom(10, 20, 30)));
        }
        return request;
    }

    /**
     * This asserts the index is fully replicated from the leader index to the follower index. It first verifies that the seq_no_stats
     * on the follower equal the leader's; then verifies the existing pairs of (docId, seqNo) on the follower also equal the leader.
     */
    protected void assertIndexFullyReplicatedToFollower(String leaderIndex, String followerIndex) throws Exception {
        logger.info("--> asserting <<docId,seqNo>> between {} and {}", leaderIndex, followerIndex);
        assertBusy(() -> {
            Map<Integer, List<DocIdSeqNoAndSource>> docsOnFollower = getDocIdAndSeqNos(clusterGroup.followerCluster, followerIndex);
            Map<Integer, List<DocIdSeqNoAndSource>> docsOnLeader = getDocIdAndSeqNos(clusterGroup.leaderCluster, leaderIndex);
            Map<Integer, Set<DocIdSeqNoAndSource>> mismatchedDocs = new HashMap<>();
            for (Map.Entry<Integer, List<DocIdSeqNoAndSource>> fe : docsOnFollower.entrySet()) {
                Set<DocIdSeqNoAndSource> d1 = Sets.difference(
                    Sets.newHashSet(fe.getValue()),
                    Sets.newHashSet(docsOnLeader.getOrDefault(fe.getKey(), Collections.emptyList()))
                );
                Set<DocIdSeqNoAndSource> d2 = Sets.difference(
                    Sets.newHashSet(docsOnLeader.getOrDefault(fe.getKey(), Collections.emptyList())),
                    Sets.newHashSet(fe.getValue())
                );
                if (d1.isEmpty() == false || d2.isEmpty() == false) {
                    mismatchedDocs.put(fe.getKey(), Sets.union(d1, d2));
                }
            }
            assertThat("mismatched documents [" + mismatchedDocs + "]", docsOnFollower, equalTo(docsOnLeader));
        }, 120, TimeUnit.SECONDS);

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
            assertThat(followerStats, equalTo(leaderStats));
        }, 120, TimeUnit.SECONDS);
    }

    private Map<Integer, List<DocIdSeqNoAndSource>> getDocIdAndSeqNos(InternalTestCluster cluster, String index) throws IOException {
        final ClusterState state = cluster.client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        List<ShardRouting> shardRoutings = state.routingTable().allShards(index);
        Randomness.shuffle(shardRoutings);
        final Map<Integer, List<DocIdSeqNoAndSource>> docs = new HashMap<>();
        for (ShardRouting shardRouting : shardRoutings) {
            if (shardRouting == null || shardRouting.assignedToNode() == false) {
                continue;
            }
            final var indexService = cluster.getInstance(IndicesService.class, state.nodes().get(shardRouting.currentNodeId()).getName())
                .indexService(shardRouting.index());
            if (indexService == null) {
                continue;
            }
            final var indexShard = indexService.getShardOrNull(shardRouting.id());
            if (indexShard == null || indexShard.routingEntry().started() == false) {
                continue;
            }
            try {
                final List<DocIdSeqNoAndSource> docsOnShard = IndexShardTestCase.getDocIdAndSeqNos(indexShard);
                logger.info("--> shard {} docs {} seq_no_stats {}", shardRouting, docsOnShard, indexShard.seqNoStats());
                docs.put(
                    shardRouting.shardId().id(),
                    docsOnShard.stream()
                        // normalize primary term as the follower use its own term
                        .map(d -> new DocIdSeqNoAndSource(d.id(), d.source(), d.seqNo(), 1L, d.version()))
                        .collect(Collectors.toList())
                );
            } catch (AlreadyClosedException e) {
                // Ignore this exception and try getting List<DocIdSeqNoAndSource> from other IndexShard instance.
            }
        }
        return docs;
    }

    protected void atLeastDocsIndexed(Client client, String index, long numDocsReplicated) throws Exception {
        logger.info("waiting for at least [{}] documents to be indexed into index [{}]", numDocsReplicated, index);
        assertBusy(() -> {
            refresh(client, index);
            SearchRequest request = new SearchRequest(index);
            request.source(new SearchSourceBuilder().size(0));
            assertResponse(client.search(request), response -> {
                assertNotNull(response.getHits().getTotalHits());
                assertThat(response.getHits().getTotalHits().value(), greaterThanOrEqualTo(numDocsReplicated));
            });
        }, 60, TimeUnit.SECONDS);
    }

    protected void awaitGlobalCheckpointAtLeast(Client client, ShardId shardId, long minimumGlobalCheckpoint) throws Exception {
        logger.info("waiting for the global checkpoint on [{}] at least [{}]", shardId, minimumGlobalCheckpoint);
        assertBusy(() -> {
            ShardStats stats = client.admin()
                .indices()
                .prepareStats(shardId.getIndexName())
                .clear()
                .get()
                .asMap()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().shardId().equals(shardId))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
            if (stats == null || stats.getSeqNoStats() == null) {
                throw new AssertionError("seq_no_stats for shard [" + shardId + "] is not found"); // causes assertBusy to retry
            }
            assertThat(
                Strings.toString(stats.getSeqNoStats()),
                stats.getSeqNoStats().getGlobalCheckpoint(),
                greaterThanOrEqualTo(minimumGlobalCheckpoint)
            );
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

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs number of documents to wait for
     * @param indexer a {@link org.elasticsearch.test.BackgroundIndexer}. Will be first checked for documents indexed.
     *                This saves on unneeded searches.
     */
    public void waitForDocs(final long numDocs, final BackgroundIndexer indexer) throws Exception {
        // indexing threads can wait for up to ~1m before retrying when they first try to index into a shard which is not STARTED.
        final long maxWaitTimeMs = Math.max(90 * 1000, 200 * numDocs);

        assertBusy(() -> {
            long lastKnownCount = indexer.totalIndexedDocs();

            if (lastKnownCount >= numDocs) {
                try {
                    long count = SearchResponseUtils.getTotalHitsValue(
                        indexer.getClient().prepareSearch().setTrackTotalHits(true).setSize(0).setQuery(QueryBuilders.matchAllQuery())
                    );

                    if (count == lastKnownCount) {
                        // no progress - try to refresh for the next time
                        indexer.getClient().admin().indices().prepareRefresh().get();
                    }
                    lastKnownCount = count;
                } catch (Exception e) { // count now acts like search and barfs if all shards failed...
                    logger.debug("failed to executed count", e);
                    throw e;
                }
            }

            if (logger.isDebugEnabled()) {
                if (lastKnownCount < numDocs) {
                    logger.debug("[{}] docs indexed. waiting for [{}]", lastKnownCount, numDocs);
                } else {
                    logger.debug("[{}] docs visible for search (needed [{}])", lastKnownCount, numDocs);
                }
            }

            assertThat(lastKnownCount, greaterThanOrEqualTo(numDocs));
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);
    }

    protected PlainActionFuture<RestoreInfo> startRestore(
        ClusterService clusterService,
        RestoreService restoreService,
        RestoreSnapshotRequest restoreSnapshotRequest
    ) {
        final var future = new PlainActionFuture<RestoreInfo>();
        restoreService.restoreSnapshot(restoreSnapshotRequest, future.delegateFailure((delegate, restoreCompletionResponse) -> {
            assertNull(restoreCompletionResponse.restoreInfo());
            // this would only be non-null if the restore was a no-op, but that would be a test bug
            final Snapshot snapshot = restoreCompletionResponse.snapshot();
            final String uuid = restoreCompletionResponse.uuid();
            final ClusterStateListener clusterStateListener = new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent changedEvent) {
                    final RestoreInProgress.Entry prevEntry = restoreInProgress(changedEvent.previousState(), uuid);
                    final RestoreInProgress.Entry newEntry = restoreInProgress(changedEvent.state(), uuid);

                    assertNotNull(prevEntry);
                    // prevEntry could be null if there was a master failover and (due to batching) we missed the cluster state update
                    // that completed the restore, but that doesn't happen in these tests
                    if (newEntry == null) {
                        clusterService.removeListener(this);
                        Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = prevEntry.shards();
                        RestoreInfo ri = new RestoreInfo(
                            prevEntry.snapshot().getSnapshotId().getName(),
                            prevEntry.indices(),
                            shards.size(),
                            shards.size() - RestoreService.failedShards(shards)
                        );
                        logger.debug("restore of [{}] completed", snapshot);
                        delegate.onResponse(ri);
                    } // else restore not completed yet, wait for next cluster state update
                }
            };
            clusterService.addListener(clusterStateListener);
        }));
        return future;
    }

    static void removeCCRRelatedMetadataFromClusterState(ClusterService clusterService) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitUnbatchedStateUpdateTask("remove-ccr-related-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                AutoFollowMetadata empty = new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
                ClusterState.Builder newState = ClusterState.builder(currentState);
                newState.putProjectMetadata(
                    ProjectMetadata.builder(currentState.metadata().getProject())
                        .putCustom(AutoFollowMetadata.TYPE, empty)
                        .removeCustom(PersistentTasksCustomMetadata.TYPE)
                        .build()
                );
                return newState.build();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
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
            IOUtils.close(CloseableTestClusterWrapper.wrap(leaderCluster, followerCluster));
        }
    }
}
