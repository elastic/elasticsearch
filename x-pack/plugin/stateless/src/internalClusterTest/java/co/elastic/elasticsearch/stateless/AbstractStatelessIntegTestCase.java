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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.ThreadPoolMergeScheduler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public abstract class AbstractStatelessIntegTestCase extends ESIntegTestCase {
    private int uploadMaxCommits;

    @Before
    public void initUploadMaxCommits() {
        uploadMaxCommits = randomBoolean()
            ? between(1, 10)
            : StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getDefault(Settings.EMPTY);
    }

    public int getUploadMaxCommits() {
        return uploadMaxCommits;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    public static final String SYSTEM_INDEX_NAME = ".sys-idx";

    protected void createSystemIndex(Settings indexSettings) {
        createIndex(SYSTEM_INDEX_NAME, indexSettings);
    }

    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(SYSTEM_INDEX_NAME + "*")
                    .setDescription("Test system indices")
                    .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                    .build()
            );
        }

        @Override
        public String getFeatureName() {
            return SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin with test indices";
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SystemIndexTestPlugin.class, BlobCachePlugin.class, Stateless.class, MockTransportService.TestPlugin.class);
    }

    public static class NoopSharedBlobCacheWarmingService extends SharedBlobCacheWarmingService {
        public NoopSharedBlobCacheWarmingService(StatelessSharedBlobCacheService cacheService, ThreadPool threadPool, Settings settings) {
            super(cacheService, threadPool, TelemetryProvider.NOOP, settings);
        }

        @Override
        protected void warmCacheRecovery(
            Type type,
            IndexShard indexShard,
            StatelessCompoundCommit commit,
            BlobStoreCacheDirectory blobStoreCacheDirectory,
            ActionListener<Void> listener
        ) {
            listener.onResponse(null);
        }

        @Override
        public void warmCacheBeforeUpload(VirtualBatchedCompoundCommit vbcc, ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        if (internalCluster().size() > 0) {
            flush(".*", "*"); // clear out index commits that could be held by current VBCC
        }
        super.beforeIndexDeletion();
    }

    protected static OperationPurpose operationPurpose;

    @BeforeClass
    public static void setupClass() {
        operationPurpose = randomFrom(OperationPurpose.values());
    }

    private boolean useBasePath;

    @Before
    public void setup() {
        useBasePath = randomBoolean();
    }

    public static void createRepository(Logger logger, String repoName, String type, Settings.Builder settings, boolean verify) {
        logger.info("--> creating or updating repository [{}] [{}]", repoName, type);
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                .setVerify(verify)
                .setType(type)
                .setSettings(settings)
        );
    }

    protected void createRepository(String repoName, String type) {
        createRepository(logger, repoName, type);
    }

    public static void createRepository(Logger logger, String repoName, String type) {
        createRepository(logger, repoName, type, randomRepositorySettings(), true);
    }

    protected void deleteRepository(String repoName) {
        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName));
    }

    protected SnapshotInfo createSnapshot(String repositoryName, String snapshot, List<String> indices, List<String> featureStates) {
        logger.info("--> creating snapshot [{}] of {} in [{}]", snapshot, indices, repositoryName);
        final CreateSnapshotResponse response = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshot)
            .setIndices(indices.toArray(Strings.EMPTY_ARRAY))
            .setWaitForCompletion(true)
            .setFeatureStates(featureStates.toArray(Strings.EMPTY_ARRAY))
            .get();

        final SnapshotInfo snapshotInfo = response.getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
        return snapshotInfo;
    }

    private static Settings.Builder randomRepositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", randomRepoPath()).put("compress", randomBoolean());
        return settings;
    }

    protected String getFsRepoSanitizedBucketName() {
        return getTestName().replaceAll("[^0-9a-zA-Z-_]", "_") + "_bucket";
    }

    /**
     * Set a very high {@link StoreHeartbeatService#MAX_MISSED_HEARTBEATS} value by default so that the cluster does not recover from an
     * <i>unexpected</i> master failover before the whole suite times out. Tests that expect to see a master failover should generally use
     * {@link #shutdownMasterNodeGracefully} to trigger the usual graceful abdication process. If a test really needs to simulate an abrupt
     * master failure then it should adjust the heartbeat configuration by setting both {@link StoreHeartbeatService#MAX_MISSED_HEARTBEATS}
     * and {@link StoreHeartbeatService#HEARTBEAT_FREQUENCY} to ensure that the test cluster recovers without undue delay.
     */
    private static final int DEFAULT_TEST_MAX_MISSED_HEARTBEATS = 1000;

    protected Settings.Builder nodeSettings() {
        final Settings.Builder builder = Settings.builder()
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .put(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), false)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), getFsRepoSanitizedBucketName())
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), DEFAULT_TEST_MAX_MISSED_HEARTBEATS)
            .put(ThreadPoolMergeScheduler.MERGE_THREAD_POOL_SCHEDULER.getKey(), randomBoolean())
            .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), randomBoolean());
        if (useBasePath) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), "base_path");
        }
        builder.put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), getUploadMaxCommits());
        return builder;
    }

    protected String startIndexNode() {
        return startIndexNode(Settings.EMPTY);
    }

    protected String startIndexNode(Settings extraSettings) {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.INDEX_ROLE).put(extraSettings));
    }

    protected String startSearchNode() {
        return startSearchNode(Settings.EMPTY);
    }

    protected String startSearchNode(Settings extraSettings) {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.SEARCH_ROLE).put(extraSettings));
    }

    protected Settings.Builder settingsForRoles(DiscoveryNodeRole... roles) {
        var builder = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), Arrays.stream(roles).map(DiscoveryNodeRole::roleName).toList());

        // when changing those values, keep in mind that multiple nodes with their own caches can be created by integration tests which can
        // also be executed concurrently.
        if (frequently()) {
            if (randomBoolean()) {
                // region is between 1 page (4kb) to 8 pages (32kb)
                var regionPages = randomIntBetween(1, 8);
                builder.put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes((long) regionPages * SharedBytes.PAGE_SIZE));
                // cache is between 1 and 256 regions (max. possible cache size is 8mb)
                builder.put(
                    SHARED_CACHE_SIZE_SETTING.getKey(),
                    ByteSizeValue.ofBytes((long) randomIntBetween(regionPages, 256) * SharedBytes.PAGE_SIZE)
                );
            } else {
                if (randomBoolean()) {
                    // region is between 8 pages (32kb) to 256 pages (1mb)
                    var regionPages = randomIntBetween(8, 256);
                    builder.put(
                        SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                        ByteSizeValue.ofBytes((long) regionPages * SharedBytes.PAGE_SIZE)
                    );
                }
                // cache only uses up to 0.1% disk to be friendly with default region size
                builder.put(SHARED_CACHE_SIZE_SETTING.getKey(), new RatioValue(randomDoubleBetween(0.0d, 0.1d, false)).toString());
            }
        } else {
            // no cache (a single region does not even fit in the cache)
            builder.put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(1L));
        }

        // Add settings from nodeSettings last, allowing them to take precedence over the randomly generated values
        return builder.put(nodeSettings().build());
    }

    protected String startMasterOnlyNode() {
        return startMasterOnlyNode(Settings.EMPTY);
    }

    protected String startMasterOnlyNode(Settings extraSettings) {
        return internalCluster().startMasterOnlyNode(nodeSettings().put(extraSettings).build());
    }

    protected String startMasterAndIndexNode() {
        return startMasterAndIndexNode(Settings.EMPTY);
    }

    protected String startMasterAndIndexNode(Settings extraSettings) {
        return internalCluster().startNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE).put(extraSettings)
        );
    }

    protected List<String> startIndexNodes(int numOfNodes) {
        return startIndexNodes(numOfNodes, Settings.EMPTY);
    }

    protected List<String> startIndexNodes(int numOfNodes, Settings extraSettings) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startIndexNode(extraSettings));
        }
        return List.copyOf(nodes);
    }

    protected List<String> startSearchNodes(int numOfNodes) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startSearchNode());
        }
        return List.copyOf(nodes);
    }

    protected String startSearchNode(StatelessMockRepositoryStrategy strategy) {
        return startSearchNode(Settings.EMPTY, strategy);
    }

    protected String startSearchNode(Settings extraSettings, StatelessMockRepositoryStrategy strategy) {
        var nodeName = internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.SEARCH_ROLE).put(extraSettings));
        setNodeRepositoryStrategy(nodeName, strategy);
        return nodeName;
    }

    protected String startIndexNode(Settings extraSettings, StatelessMockRepositoryStrategy strategy) {
        var nodeName = internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.INDEX_ROLE).put(extraSettings));
        setNodeRepositoryStrategy(nodeName, strategy);
        return nodeName;
    }

    protected String startMasterAndIndexNode(Settings extraSettings, StatelessMockRepositoryStrategy strategy) {
        var nodeName = startMasterAndIndexNode(extraSettings);
        setNodeRepositoryStrategy(nodeName, strategy);
        return nodeName;
    }

    protected String startMasterAndIndexNode(StatelessMockRepositoryStrategy strategy) {
        return startMasterAndIndexNode(Settings.EMPTY, strategy);
    }

    protected String startMasterOnlyNode(StatelessMockRepositoryStrategy strategy) {
        var nodeName = startMasterOnlyNode(Settings.EMPTY);
        setNodeRepositoryStrategy(nodeName, strategy);
        return nodeName;
    }

    protected void setNodeRepositoryStrategy(String nodeName, StatelessMockRepositoryStrategy strategy) {
        ObjectStoreService objectStoreService = getObjectStoreService(nodeName);
        ObjectStoreTestUtils.getObjectStoreStatelessMockRepository(objectStoreService).setStrategy(strategy);
    }

    protected StatelessMockRepositoryStrategy getNodeRepositoryStrategy(String nodeName) {
        ObjectStoreService objectStoreService = getObjectStoreService(nodeName);
        return ObjectStoreTestUtils.getObjectStoreStatelessMockRepository(objectStoreService).getStrategy();
    }

    protected static BulkResponse indexDocs(String indexName, int numDocs) {
        return indexDocs(indexName, numDocs, UnaryOperator.identity());
    }

    /**
     * Initiates and waits for the elected-master to gracefully abdicate to another master-eligible node before shutting it down.
     * Graceful shutdown can only be successful if there is at least one other master-eligible node to which to abdicate.
     *
     * @return the name of the master node that is shut down.
     */
    public static String shutdownMasterNodeGracefully() throws Exception {
        String masterNodeName = masterNodeAbdicatesForGracefulShutdown();
        internalCluster().stopNode(masterNodeName);
        return masterNodeName;
    }

    /**
     * Initiates and waits for the elected-master to gracefully abdicate to another master-eligible node before restarting it.
     * Graceful shutdown can only be successful if there is at least one other master-eligible node to which to abdicate.
     *
     * @return the name of the restarted master node.
     */
    public static String restartMasterNodeGracefully() throws Exception {
        String masterNodeName = masterNodeAbdicatesForGracefulShutdown();
        internalCluster().restartNode(masterNodeName);
        return masterNodeName;
    }

    /**
     * Initiates and waits for the elected-master to gracefully abdicate to another master-eligible node.
     * Note: it is asserted that no other nodes in the cluster are preparing to shut down.
     *
     * @return the name of the master node that abdicated.
     */
    private static String masterNodeAbdicatesForGracefulShutdown() {
        // Ensure that there is at least one other master role node to which the current master can abdicate.
        assertThat(
            "Master node cannot abdicate gracefully on shutdown when there is no other master-eligible node",
            internalCluster().numMasterNodes(),
            greaterThan(1)
        );

        final String masterNodeName = internalCluster().getMasterName();

        // Create a listener for new master elections.
        final var nextMasterElectedLatch = new CountDownLatch(1);
        final ClusterStateListener newMasterListener = clusterChangedEvent -> {
            if (clusterChangedEvent.localNodeMaster()) {
                nextMasterElectedLatch.countDown();
            }
        };
        // Add the master election listener to all the not-current-master nodes.
        for (var clusterService : internalCluster().getInstances(ClusterService.class)) {
            if (clusterService.localNode().getName().equals(masterNodeName) == false) {
                clusterService.addListener(newMasterListener);
            }
        }

        // Update the cluster state as a PutShutdown action would, so the node will abdicate its role as master in preparation for shutdown.
        internalCluster().getInstance(ClusterService.class, masterNodeName)
            .submitUnbatchedStateUpdateTask("add shutdown for test", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertThat(currentState.metadata().nodeShutdowns().getAll().size(), equalTo(0));

                    // Create a new map for shutdown state and add the master node as preparing to restart.
                    var shutdownMetadata = Map.of(
                        currentState.nodes().getMasterNodeId(),
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(currentState.nodes().getMasterNodeId())
                            .setType(SingleNodeShutdownMetadata.Type.RESTART)
                            .setStartedAtMillis(randomNonNegativeLong())
                            .setReason("master failover for test")
                            .build()
                    );

                    return currentState.copyAndUpdateMetadata(
                        metadata -> metadata.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownMetadata))
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });

        // Wait for one of the other master-eligible nodes to be elected as the new master.
        safeAwait(nextMasterElectedLatch);

        // Remove the cluster service listeners.
        for (var clusterService : internalCluster().getInstances(ClusterService.class)) {
            if (clusterService.localNode().getName().equals(masterNodeName) == false) {
                clusterService.removeListener(newMasterListener);
            }
        }

        // Remove the shutdown state for the old master.
        internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName())
            .submitUnbatchedStateUpdateTask("remove shutdown for test", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertThat(currentState.metadata().nodeShutdowns().getAll().size(), equalTo(1));

                    // Install an empty map of nodes-to-shutdown-metadata, thus moving the old master node out of shut down mode.
                    return currentState.copyAndUpdateMetadata(
                        metadata -> metadata.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of()))
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });

        return masterNodeName;
    }

    protected static BulkResponse indexDocs(String indexName, int numDocs, UnaryOperator<BulkRequestBuilder> requestOperator) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        var bulkResponse = requestOperator.apply(bulkRequest).get();
        assertNoFailures(bulkResponse);
        return bulkResponse;
    }

    protected void indexDocsAndRefresh(String indexName, int numDocs) throws Exception {
        indexDocsAndRefresh(client(), indexName, numDocs);
    }

    protected void indexDocsAndRefresh(Client client, String indexName, int numDocs) throws Exception {
        var bulkRequest = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        boolean bulkRefreshes = randomBoolean();
        if (bulkRefreshes) {
            bulkRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        }
        assertNoFailures(bulkRequest.get());
        if (bulkRefreshes == false) {
            assertNoFailures(client.admin().indices().prepareRefresh(indexName).execute().get());
        }
    }

    @Override
    protected boolean addMockFSIndexStore() {
        return false;
    }

    protected static ObjectStoreService getCurrentMasterObjectStoreService() {
        return internalCluster().getCurrentMasterNodeInstance(StatelessComponents.class).getObjectStoreService();
    }

    protected static ObjectStoreService getObjectStoreService(String nodeName) {
        return internalCluster().getInstance(StatelessComponents.class, nodeName).getObjectStoreService();
    }

    protected static TranslogReplicator getTranslogReplicator(String nodeName) {
        return internalCluster().getInstance(StatelessComponents.class, nodeName).getTranslogReplicator();
    }

    protected static void indexDocumentsThenFlushOrRefreshOrForceMerge(String indexName) {
        indexDocumentsThenFlushOrRefreshOrForceMerge(indexName, () -> {}, () -> {}, () -> {});
    }

    protected static void indexDocumentsThenFlushOrRefreshOrForceMerge(
        String indexName,
        Runnable afterFlush,
        Runnable afterRefresh,
        Runnable afterForceMerge
    ) {
        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> {
                    client().admin().indices().prepareFlush(indexName).setForce(randomBoolean()).get();
                    afterFlush.run();
                }
                case 1 -> {
                    client().admin().indices().prepareRefresh(indexName).get();
                    afterRefresh.run();
                }
                case 2 -> {
                    client().admin().indices().prepareForceMerge(indexName).get();
                    afterForceMerge.run();
                }
            }
        }
    }

    protected static DiscoveryNode findIndexNode(Index index, int shardId) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), DiscoveryNodeRole.INDEX_ROLE)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shardOrNull = indexService.getShardOrNull(shardId);
                    if (shardOrNull != null && shardOrNull.isActive()) {
                        assertTrue(shardOrNull.routingEntry().primary());
                        return indicesService.clusterService().localNode();
                    }
                }
            }
        }
        throw new AssertionError("Cannot finding indexing node for: " + shardId);
    }

    protected static IndexShard findIndexShard(String indexName) {
        final Map<Index, Integer> indices = resolveIndices();
        Index index = indices.entrySet().stream().filter(e -> e.getKey().getName().equals(indexName)).findAny().get().getKey();
        IndexShard indexShard = findIndexShard(index, 0);
        return indexShard;
    }

    protected static IndexShard findIndexShard(Index index, int shardId) {
        return findShard(index, shardId, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
    }

    protected static IndexShard findSearchShard(String indexName) {
        final Map<Index, Integer> indices = resolveIndices();
        Index index = indices.entrySet().stream().filter(e -> e.getKey().getName().equals(indexName)).findAny().get().getKey();
        IndexShard indexShard = findSearchShard(index, 0);
        return indexShard;
    }

    protected static IndexShard findSearchShard(Index index, int shardId) {
        return findShard(index, shardId, DiscoveryNodeRole.SEARCH_ROLE, ShardRouting.Role.SEARCH_ONLY);
    }

    protected static IndexShard findShard(Index index, int shardId, DiscoveryNodeRole nodeRole, ShardRouting.Role shardRole) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), nodeRole)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shard = indexService.getShardOrNull(shardId);
                    if (shard != null && shard.isActive()) {
                        assertThat("Unexpected shard role", shard.routingEntry().role(), equalTo(shardRole));
                        return shard;
                    }
                }
            }
        }
        throw new AssertionError(
            "IndexShard instance not found for shard " + new ShardId(index, shardId) + " on nodes with [" + nodeRole.roleName() + "] role"
        );
    }

    protected static IndexShard findIndexShard(Index index, int shardId, String nodeName) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexService(index);
        if (indexService != null) {
            IndexShard shard = indexService.getShardOrNull(shardId);
            if (shard != null && shard.isActive()) {
                assertThat("Unexpected shard role", shard.routingEntry().role(), equalTo(ShardRouting.Role.INDEX_ONLY));
                return shard;
            }
        }
        throw new AssertionError("IndexShard instance not found for shard " + new ShardId(index, shardId) + " on node [" + nodeName + ']');
    }

    @SuppressWarnings("unchecked")
    protected static <E extends Engine> E getShardEngine(IndexShard indexShard, Class<E> engineClass) {
        var engine = indexShard.getEngineOrNull();
        assertThat(engine, notNullValue());
        assertThat(engine, instanceOf(engineClass));
        return (E) engine;
    }

    protected static Map<Index, Integer> resolveIndices() {
        return client().admin()
            .indices()
            .prepareGetIndex()
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            .get()
            .getSettings()
            .values()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    settings -> new Index(
                        settings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME),
                        settings.get(IndexMetadata.SETTING_INDEX_UUID)
                    ),
                    settings -> settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)
                )
            );
    }

    protected static void assertReplicatedTranslogConsistentWithShards() throws Exception {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                DiscoveryNode indexNode = findIndexNode(entry.getKey(), shardId);
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                final ShardId objShardId = new ShardId(entry.getKey(), shardId);

                // Check that the translog on the object store contains the correct sequence numbers and number of operations
                var indexObjectStoreService = getObjectStoreService(indexNode.getName());
                var reader = new TranslogReplicatorReader(indexObjectStoreService.getTranslogBlobContainer(), objShardId);
                long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                long totalOps = 0;
                Translog.Operation next = reader.next();
                while (next != null) {
                    maxSeqNo = SequenceNumbers.max(maxSeqNo, next.seqNo());
                    totalOps++;
                    next = reader.next();
                }
                assertThat(maxSeqNo, equalTo(indexShard.seqNoStats().getMaxSeqNo()));
                assertThat(totalOps, equalTo(indexShard.seqNoStats().getMaxSeqNo() + 1));
            }
        }
    }

    protected Set<String> listBlobsWithAbsolutePath(BlobContainer blobContainer) throws IOException {
        var blobContainerPath = blobContainer.path().buildAsString();
        return blobContainer.listBlobs(operationPurpose)
            .keySet()
            .stream()
            .map(blob -> blobContainerPath + blob)
            .collect(Collectors.toSet());
    }

    protected static BlobContainer getShardCommitsContainerForCurrentPrimaryTerm(String indexName, String indexNode, int shardId) {
        var indexObjectStoreService = getObjectStoreService(indexNode);
        var primaryTerm = client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .index(indexName)
            .primaryTerm(shardId);
        return indexObjectStoreService.getBlobContainer(new ShardId(resolveIndex(indexName), shardId), primaryTerm);
    }

    @Override
    protected boolean autoManageVotingExclusions() {
        return false;
    }

    protected void flushNoForceNoWait(String... indexNames) {
        client().admin().indices().prepareFlush(indexNames).setForce(false).setWaitIfOngoing(false).get(TimeValue.timeValueSeconds(10));
    }

    protected static void assertNodeHasNoCurrentRecoveries(String nodeName) {
        NodesStatsResponse nodesStatsResponse = clusterAdmin().prepareNodesStats(nodeName)
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get();
        assertThat(nodesStatsResponse.getNodes(), hasSize(1));
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
        assertThat(recoveryStats.currentAsSource(), equalTo(0));
        assertThat(recoveryStats.currentAsTarget(), equalTo(0));
    }

    protected static PrimaryTermAndGeneration getIndexingShardTermAndGeneration(String indexName, int shardId) {
        final var indexShard = findIndexShard(resolveIndex(indexName), shardId);
        final var engineOrNull = indexShard.getEngineOrNull();
        assertThat(engineOrNull, notNullValue());
        return new PrimaryTermAndGeneration(indexShard.getOperationPrimaryTerm(), ((IndexEngine) engineOrNull).getCurrentGeneration());
    }

    protected static Set<PrimaryTermAndGeneration> listBlobsTermAndGenerations(ShardId shardId) throws Exception {
        Set<PrimaryTermAndGeneration> set = new HashSet<>();
        var objectStoreService = getObjectStoreService(internalCluster().getRandomNodeName());
        var indexBlobContainer = objectStoreService.getBlobContainer(shardId);
        for (var entry : indexBlobContainer.children(operationPurpose).entrySet()) {
            var primaryTerm = Long.parseLong(entry.getKey());
            Set<String> statelessCompoundCommits = entry.getValue().listBlobs(operationPurpose).keySet();
            statelessCompoundCommits.forEach(
                filename -> set.add(
                    new PrimaryTermAndGeneration(primaryTerm, StatelessCompoundCommit.parseGenerationFromBlobName(filename))
                )
            );
        }
        return set;
    }

    protected static long[] getPrimaryTerms(Client client, String indexName) {
        var response = client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
        var state = response.getState();

        var indexMetadata = state.metadata().index(indexName);
        long[] primaryTerms = new long[indexMetadata.getNumberOfShards()];
        for (int i = 0; i < primaryTerms.length; i++) {
            primaryTerms[i] = indexMetadata.primaryTerm(i);
        }
        return primaryTerms;
    }

    protected static <T> T findPlugin(String nodeName, Class<T> pluginType) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(pluginType)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Plugin not found: " + pluginType.getName()));
    }

    protected static Settings disableIndexingDiskAndMemoryControllersNodeSettings() {
        return Settings.builder()
            .put(IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.MINUS_ONE)
            .build();
    }
}
