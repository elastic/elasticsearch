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

import co.elastic.elasticsearch.serverless.multiproject.ServerlessMultiProjectPlugin;
import co.elastic.elasticsearch.serverless.multiproject.action.TransportGetProjectStatusAction;
import co.elastic.elasticsearch.settings.secure.ServerlessSecureSettingsPlugin;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.multiproject.ProjectLease;
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
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.MergeMetrics;
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
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.cluster.metadata.RepositoriesMetadata.HIDE_GENERATIONS_PARAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public abstract class AbstractStatelessIntegTestCase extends ESIntegTestCase {

    public static final boolean STATELESS_HOLLOW_ENABLED = Booleans.parseBoolean(
        System.getProperty("es.test.stateless.hollow.enabled", "false")
    );

    public static final TimeValue STATELESS_HOLLOW_DS_NON_WRITE_TTL = TimeValue.timeValueMillis(
        Long.parseLong(System.getProperty("es.test.stateless.hollow.ds_non_write_ttl_ms", "100"))
    );

    public static final TimeValue STATELESS_HOLLOW_TTL = TimeValue.timeValueMillis(
        Long.parseLong(System.getProperty("es.test.stateless.hollow.ttl_ms", "100"))
    );

    private int uploadMaxCommits;

    @Before
    public void initUploadMaxCommits() {
        uploadMaxCommits = randomBoolean()
            ? between(1, 10)
            : StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getDefault(Settings.EMPTY);
    }

    @After
    public void tearDown() throws Exception {
        // This works for stateless as we build a new cluster for each TEST. However, if we move to SUITE this might need to be in
        // AfterClass depending on the test's needs.
        waitForMergesToFinish();
        super.tearDown();
    }

    private static void waitForMergesToFinish() throws Exception {
        InternalTestCluster internalTestCluster = internalCluster();
        Iterable<MergeMetrics> mergeMetrics = internalTestCluster.getInstances(MergeMetrics.class);
        assertBusy(
            () -> assertThat(
                StreamSupport.stream(mergeMetrics.spliterator(), false)
                    .mapToLong(m -> m.getQueuedMergeSizeInBytes() + m.getRunningMergeSizeInBytes())
                    .sum(),
                equalTo(0L)
            )
        );
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
        var plugins = new ArrayList<Class<? extends Plugin>>();
        plugins.add(SystemIndexTestPlugin.class);
        plugins.add(BlobCachePlugin.class);
        plugins.add(Stateless.class);
        plugins.add(MockTransportService.TestPlugin.class);
        if (addMockFsRepository()) {
            plugins.add(ConcurrentMultiPartUploadsMockFsRepository.Plugin.class);
        }
        if (multiProjectIntegrationTest()) {
            plugins.add(ServerlessSecureSettingsPlugin.class);
            plugins.add(ServerlessMultiProjectPlugin.class);
        }
        return List.copyOf(plugins);
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

    /**
     * Indicate if the test should use a mocked FS repository implementation that supports concurrent multipart uploads
     */
    protected boolean addMockFsRepository() {
        return true;
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
            .put(DiscoveryModule.ELECTION_STRATEGY_SETTING.getKey(), StatelessElectionStrategy.NAME)
            .put(RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), false)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), getFsRepoSanitizedBucketName())
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), DEFAULT_TEST_MAX_MISSED_HEARTBEATS)
            .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), randomBoolean());
        // Defines a default object store type, which can be overridden by test suites
        if (addMockFsRepository() && randomBoolean()) {
            builder.put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
            builder.put(randomConcurrentMultiPartSettings(random(), logger));
        } else {
            builder.put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS);
        }
        if (useBasePath) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), "base_path");
        }
        builder.put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), getUploadMaxCommits());
        if (STATELESS_HOLLOW_ENABLED) {
            builder.put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true);
            builder.put(HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), STATELESS_HOLLOW_DS_NON_WRITE_TTL);
            builder.put(HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.getKey(), STATELESS_HOLLOW_TTL);
        }
        if (multiProjectIntegrationTest()) {
            builder.put(ServerlessMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true);
        }
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
     * Note: it is asserted that no other nodes in the cluster are preparing to shut down.
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
     * Note: it is asserted that no other nodes in the cluster are preparing to shut down.
     *
     * @return the name of the restarted master node.
     */
    public static String restartMasterNodeGracefully() throws Exception {
        String masterNodeName = masterNodeAbdicatesForGracefulShutdown();
        internalCluster().restartNode(masterNodeName);
        return masterNodeName;
    }

    /**
     * Waits for all the nodes flagged for shutdown to no longer be master.
     * Asserts that there is at least one master eligible node NOT in shutdown.
     * It is safe to call this method when no nodes are in shutdown and {@code expectNodesInShutdown=false}: it will simply return once an
     * elected master node is detected.
     * @param expectNodesInShutdown true if the caller expects one or more nodes to be in shutdown, false if there might be nodes in
     *                              shutdown, but it is ok if there are none.
     * @return The elected master node name.
     */
    public String waitForAnyShuttingDownMasterNodesToAbdicateAndElectANewMaster(boolean expectNodesInShutdown) throws Exception {
        final var masterName = new AtomicReference<String>();
        final var allMasterEligibleNodesInShutdownErrorString = new AtomicReference<String>();
        final var nodeIdsToNamesMap = nodeIdsToNames();
        final var masterNodeNames = internalCluster().masterEligibleNodeNames();

        final Function<Set<String>, Boolean> allMasterEligibleNodesInShutdown = (shuttingDownNodeIds) -> {
            final var shuttingDownNodesNames = shuttingDownNodeIds.stream().map(nodeIdsToNamesMap::get).collect(Collectors.toSet());
            final var shuttingDownMasterNodeNames = shuttingDownNodesNames.stream()
                .filter(masterNodeNames::contains)
                .collect(Collectors.toSet());

            // Ensure that there is at least one master-eligible node not in shutdown.
            if (masterNodeNames.size() > shuttingDownMasterNodeNames.size()) {
                return false;
            }

            allMasterEligibleNodesInShutdownErrorString.set(Strings.format("""
                    Master node(s) cannot abdicate gracefully on shutdown when there are no other master-eligible nodes.
                    Nodes with shutdown set [%s]. All nodes with master role [%s]. Master nodes with shutdown set [%s].
                """, shuttingDownNodesNames, masterNodeNames, shuttingDownMasterNodeNames));
            return true;
        };

        // Wait for an elected master that is not shutting down.
        final var newMasterListener = ClusterServiceUtils.addTemporaryStateListener(state -> {
            final var shuttingDownNodeIds = state.metadata().nodeShutdowns().getAllNodeIds();

            if (expectNodesInShutdown) {

                if (shuttingDownNodeIds.isEmpty()) {
                    return false;
                }

                // Exit from the state listener early if we detect that all master eligible nodes are in shutdown.
                if (allMasterEligibleNodesInShutdown.apply(shuttingDownNodeIds)) {
                    return true;
                }
            }

            final var newMasterNode = state.nodes().getMasterNode();
            if (Optional.ofNullable(newMasterNode).map(m -> shuttingDownNodeIds.contains(m.getId()) == false).orElse(false)) {
                masterName.set(newMasterNode.getName());
                return true;
            }

            return false;
        });

        safeAwait(newMasterListener);

        if (allMasterEligibleNodesInShutdownErrorString.get() != null) {
            fail(allMasterEligibleNodesInShutdownErrorString.get());
        }

        return masterName.get();
    }

    /**
     * Initiates and waits for the elected-master to gracefully abdicate to another master-eligible node.
     * Note: it is asserted that no other nodes in the cluster are preparing to shut down.
     *
     * @return the name of the master node that abdicated.
     */
    protected static String masterNodeAbdicatesForGracefulShutdown() {
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

    protected static BulkResponse indexDocs(String indexName, int numDocs, Supplier<String> docIdSupplier) {
        return indexDocs(indexName, numDocs, UnaryOperator.identity(), docIdSupplier, null);
    }

    protected static BulkResponse indexDocs(String indexName, int numDocs, UnaryOperator<BulkRequestBuilder> requestOperator) {
        return indexDocs(indexName, numDocs, requestOperator, null, null);
    }

    protected static <T> BulkResponse indexDocs(
        String indexName,
        int numDocs,
        UnaryOperator<BulkRequestBuilder> bulkRequestOperator,
        @Nullable Supplier<String> docIdSupplier,
        @Nullable Supplier<Map<String, ?>> sourceSupplier
    ) {
        final var client = client();
        var bulkRequest = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            var indexRequest = client.prepareIndex(indexName);
            if (docIdSupplier != null) {
                indexRequest.setId(Objects.requireNonNull(docIdSupplier.get()));
            }
            Map<String, ?> source;
            if (sourceSupplier == null) {
                source = Map.of("field", randomUnicodeOfCodepointLengthBetween(1, 25));
            } else {
                source = sourceSupplier.get();
            }
            bulkRequest.add(indexRequest.setSource(source));
        }
        var bulkResponse = bulkRequestOperator.apply(bulkRequest).get();
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
            .prepareGetIndex(TEST_REQUEST_TIMEOUT)
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
            .getProject()
            .index(indexName)
            .primaryTerm(shardId);
        return indexObjectStoreService.getProjectBlobContainer(new ShardId(resolveIndex(indexName), shardId), primaryTerm);
    }

    @Override
    protected boolean autoManageVotingExclusions() {
        return false;
    }

    protected void flushNoForceNoWait(String... indexNames) {
        client().admin().indices().prepareFlush(indexNames).setForce(false).setWaitIfOngoing(false).get(TimeValue.timeValueSeconds(10));
    }

    /**
     * Waits for all relocations and force merge all indices in the cluster to 1 segment.
     * Optionally, asserts that there is indeed a single segment on indexing shards only.
     */
    @Override
    protected BroadcastResponse forceMerge(boolean assertOneSegmentOnPrimaries) {
        var forceMergeResponse = super.forceMerge(false);
        if (assertOneSegmentOnPrimaries) {
            // after a force merge there should only be 1 segment per shard
            var shardsWithMultipleSegments = getShardSegments().stream()
                .filter(shardSegments -> shardSegments.getShardRouting().isPromotableToPrimary())
                .filter(shardSegments -> shardSegments.getSegments().size() > 1)
                .toList();
            assertTrue(
                "there are primary shards with multiple segments " + shardsWithMultipleSegments,
                shardsWithMultipleSegments.isEmpty()
            );
        }
        return forceMergeResponse;
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
        var indexBlobContainer = objectStoreService.getProjectBlobContainer(shardId);
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

        var indexMetadata = state.metadata().getProject().index(indexName);
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

    protected static TestTelemetryPlugin getTelemetryPlugin(String node) {
        return findPlugin(node, TestTelemetryPlugin.class);
    }

    protected static long getTotalLongCounterValue(String name, TestTelemetryPlugin telemetryPlugin) {
        return telemetryPlugin.getLongCounterMeasurement(name).stream().mapToLong(Measurement::getLong).sum();
    }

    protected static long getTotalLongUpDownCounterValue(String name, TestTelemetryPlugin telemetryPlugin) {
        return telemetryPlugin.getLongUpDownCounterMeasurement(name).stream().mapToLong(Measurement::getLong).sum();
    }

    protected static long getTotalLongHistogramValue(String name, TestTelemetryPlugin telemetryPlugin) {
        return telemetryPlugin.getLongHistogramMeasurement(name).stream().mapToLong(Measurement::getLong).sum();
    }

    protected static long getLastLongGaugeValue(String name, TestTelemetryPlugin telemetryPlugin) {
        List<Measurement> measurements = telemetryPlugin.getLongGaugeMeasurement(name);
        return measurements.isEmpty() ? 0L : measurements.get(measurements.size() - 1).getLong();
    }

    protected static Settings disableIndexingDiskAndMemoryControllersNodeSettings() {
        return Settings.builder()
            .put(IndexingMemoryController.SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.MINUS_ONE)
            .build();
    }

    protected void hollowShards(String indexName, int numberOfShards, String indexNodeA, String indexNodeB) throws Exception {
        var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(resolveIndex(indexName), 0);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            assertFalse(indexEngine.isLastCommitHollow());
            assertBusy(() -> assertTrue(hollowShardsServiceA.isHollowableIndexShard(indexShard)));
        }

        logger.debug("--> relocating {} hollowable shards from {} to {}", numberOfShards, indexNodeA, indexNodeB);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> {
            var nodes = internalCluster().nodesInclude(indexName);
            assertThat(nodes, not(hasItem(indexNodeA)));
            assertThat(nodes, hasItem(indexNodeB));
        });
        ensureGreen(indexName);

        var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, indexNodeB);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(resolveIndex(indexName), i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));
            hollowShardsServiceB.ensureHollowShard(indexShard.shardId(), true);
        }
    }

    protected static Settings randomConcurrentMultiPartSettings(Random random, Logger logger) {
        // 1KiB minimum to trigger a lot of concurrent uploads. The other values are the approximate 75th, 90th, 95th and 99th percentiles
        // of the batched compound commit blobs generated by IT tests
        var threshold = ByteSizeValue.of(randomFrom(1, 32, 64, 128, 512), ByteSizeUnit.KB);
        ByteSizeValue partSize;
        if (threshold.getBytes() == 1024L) {
            partSize = ByteSizeValue.of(randomLongBetween(512L, 1024L), ByteSizeUnit.BYTES);
        } else {
            partSize = ByteSizeValue.of(randomLongBetween(4096L, threshold.getBytes()), ByteSizeUnit.BYTES);
        }
        logger.info("using concurrent multipart upload with threshold [{}] and part size [{}]", threshold.getBytes(), partSize.getBytes());
        return Settings.builder()
            .put(ConcurrentMultiPartUploadsMockFsRepository.MULTIPART_UPLOAD_THRESHOLD_SIZE, threshold)
            .put(ConcurrentMultiPartUploadsMockFsRepository.MULTIPART_UPLOAD_PART_SIZE, partSize)
            .build();
    }

    private static final AtomicLong reservedStateVersionCounter = new AtomicLong(1);

    // TODO: Extract file manipulation code to an utility class, see also ES-12052
    protected void putProject(ProjectId projectId) throws Exception {
        putProject(
            projectId,
            Settings.builder()
                .put("stateless.object_store.type", "fs")
                .put("stateless.object_store.bucket", "project_" + projectId)
                .put("stateless.object_store.base_path", "base_path")
                .put("stateless.object_store.client", "default")
                .build(),
            Settings.EMPTY,
            null
        );
    }

    protected void putProject(
        ProjectId projectId,
        Settings projectSettings,
        Settings projectSecrets,
        @Nullable RepositoryMetadata repositoryMetadata
    ) throws Exception {
        assert multiProjectIntegrationTest() : "multiProjectIntegrationTest() must be overridden to true for multi-project tests";
        final var fileSettingsService = internalCluster().getCurrentMasterNodeInstance(FileSettingsService.class);
        assertTrue(fileSettingsService.watching());
        Files.createDirectories(fileSettingsService.watchedFileDir());
        final long newVersion = reservedStateVersionCounter.incrementAndGet();

        final String settingsJson = addProjectIdToSettingsJson(fileSettingsService.watchedFile(), newVersion, projectId.id());
        final String repositoryJson = getRepositoryJson(repositoryMetadata);
        final var projectSettingsJson = Strings.format("""
            {
                 "metadata": {
                     "version": "%s",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "project_settings": %s,
                     "snapshot_repositories": %s
                 }
            }""", newVersion, projectSettings.toString(), repositoryJson);

        final var projectSecretsJson = Strings.format("""
            {
                 "metadata": {
                     "version": "%s",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "project_secrets": {
                        "string_secrets": %s,
                        "file_secrets": %s
                     }
                 }
            }""", newVersion, projectSecrets.toString(), "{}");

        Files.writeString(fileSettingsService.watchedFile().resolveSibling("project-" + projectId + ".json"), projectSettingsJson);
        Files.writeString(fileSettingsService.watchedFile().resolveSibling("project-" + projectId + ".secrets.json"), projectSecretsJson);
        writeAndMoveContentAtomically(settingsJson, fileSettingsService.watchedFile());

        // Ensure the project exist
        assertBusy(() -> {
            final var request = new TransportGetProjectStatusAction.Request(TEST_REQUEST_TIMEOUT, projectId.id());
            assertThat(
                safeGet(client().execute(TransportGetProjectStatusAction.INSTANCE, request)).getProjectId(),
                equalTo(projectId.id())
            );
        });

        // Ensure the latest update is processed
        safeAwait(
            ClusterServiceUtils.addTemporaryStateListener(
                clusterState -> Objects.equals(
                    clusterState.metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE).version(),
                    newVersion
                )
            )
        );

        // TODO: Wait for the lease to be claimed. This is needed until we have ES-11206
        final var blobContainer = getCurrentMasterObjectStoreService().getClusterRootContainer();
        final var clusterUuid = internalCluster().clusterService().state().metadata().clusterUUID();
        final var projectLeaseBlobName = ProjectLease.leaseBlobName(projectId);
        assertBusy(() -> {
            assertTrue(blobContainer.blobExists(OperationPurpose.CLUSTER_STATE, projectLeaseBlobName));
            final ProjectLease projectLease = readProjectLease(projectLeaseBlobName);
            assertTrue(projectLease.isAssigned());
            assertEquals(projectLease.clusterUuidAsString(), clusterUuid);
        });
    }

    @SuppressWarnings("unchecked")
    protected void removeProject(ProjectId projectId) throws Exception {
        assert multiProjectIntegrationTest() : "multiProjectIntegrationTest() must be overridden to true for multi-project tests";

        logger.info("--> marking project [{}] for deletion", projectId);
        final var settingsJsonPath = getFileSettingsWatchedFile();
        final var newVersion = nextReservedStateVersion();

        // Mark the project for deletion
        // Project secrets file
        final var projectSecretsPath = settingsJsonPath.resolveSibling("project-" + projectId + ".secrets.json");
        {
            final var map = XContentHelper.convertToMap(JSON.xContent(), Files.readString(projectSecretsPath), false);
            final var metadata = (Map<String, Object>) map.get("metadata");
            metadata.put("version", Strings.format("%s", newVersion));
            writeAndMoveContentAtomically(
                XContentHelper.convertToJson(XContentTestUtils.convertToXContent(map, JSON), false, XContentType.JSON),
                projectSecretsPath
            );
        }
        // Project settings file
        final var projectSettingsPath = settingsJsonPath.resolveSibling("project-" + projectId + ".json");
        {
            final var map = XContentHelper.convertToMap(JSON.xContent(), Files.readString(projectSettingsPath), false);
            final var state = (Map<String, Object>) map.get("state");
            state.put("marked_for_deletion", true); // mark for deletion
            final var metadata = (Map<String, Object>) map.get("metadata");
            metadata.put("version", Strings.format("%s", newVersion));
            writeAndMoveContentAtomically(
                XContentHelper.convertToJson(XContentTestUtils.convertToXContent(map, JSON), false, XContentType.JSON),
                projectSettingsPath
            );
        }

        // Wait for lease to be released and project metadata to be removed
        final var blobContainer = getCurrentMasterObjectStoreService().getClusterRootContainer();
        final var projectLeaseBlobName = ProjectLease.leaseBlobName(projectId);
        assertBusy(() -> {
            assertTrue(blobContainer.blobExists(OperationPurpose.CLUSTER_STATE, projectLeaseBlobName));
            final ProjectLease projectLease = readProjectLease(projectLeaseBlobName);
            assertFalse(projectLease.isAssigned()); // Lease is released
            assertArrayEquals(ProjectLease.NIL_UUID, projectLease.clusterUuid());
            assertFalse(internalCluster().getInstance(ClusterService.class).state().metadata().hasProject(projectId));
        });

        // Clean up config files after project deletion
        Files.deleteIfExists(projectSecretsPath);
        Files.deleteIfExists(projectSettingsPath);
        final String settingsJson = removeProjectIdFromSettingsJson(
            settingsJsonPath,
            reservedStateVersionCounter.incrementAndGet(),
            projectId.id()
        );
        writeAndMoveContentAtomically(settingsJson, settingsJsonPath);
        // TODO: Manually clean up the project registry. Otherwise the same project cannot be recreated due to the marked_for_deletion flag.
        // This is temporary because the clean-up should happen automatically when the project config files are removed. See also ES-12411
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitUnbatchedStateUpdateTask("delete-project-from-state-registry", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState)
                    .putCustom(
                        ProjectStateRegistry.TYPE,
                        ProjectStateRegistry.builder(ProjectStateRegistry.get(currentState)).removeProject(projectId).build()
                    )
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e, "fail to delete project from state registry");
            }
        });
        awaitClusterState(state -> ProjectStateRegistry.get(state).hasProject(projectId) == false);
    }

    protected Path getFileSettingsWatchedFile() throws IOException {
        assert multiProjectIntegrationTest() : "multiProjectIntegrationTest() must be overridden to true for multi-project tests";
        final var fileSettingsService = internalCluster().getCurrentMasterNodeInstance(FileSettingsService.class);
        assertTrue(fileSettingsService.watching());
        Files.createDirectories(fileSettingsService.watchedFileDir());
        return fileSettingsService.watchedFile();
    }

    protected long nextReservedStateVersion() {
        assert multiProjectIntegrationTest() : "multiProjectIntegrationTest() must be overridden to true for multi-project tests";
        return reservedStateVersionCounter.incrementAndGet();
    }

    protected void writeAndMoveContentAtomically(String content, Path targetPath) throws IOException {
        final Path tempFilePath = createTempFile();
        logger.info("--> atomically writing content [{}] to [{}]", content, targetPath);
        Files.writeString(tempFilePath, content);
        Files.move(tempFilePath, targetPath, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String getRepositoryJson(RepositoryMetadata repositoryMetadata) throws IOException {
        if (repositoryMetadata == null) {
            return "{}";
        }
        final var xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        RepositoriesMetadata.toXContent(
            repositoryMetadata,
            xContentBuilder,
            new ToXContent.MapParams(Map.of(HIDE_GENERATIONS_PARAM, "true"))
        );
        xContentBuilder.endObject();
        return Strings.toString(xContentBuilder);
    }

    private String addProjectIdToSettingsJson(Path settingsJsonPath, long newVersion, String projectId) throws IOException {
        return updateSettingsJson(settingsJsonPath, newVersion, map -> {
            @SuppressWarnings("unchecked")
            final var projects = new HashSet<>((Collection<String>) map.getOrDefault("projects", Set.of()));
            final var added = projects.add(projectId);
            logger.info("--> {} project [{}]", added ? "add" : "update", projectId);
            map.put("projects", projects);
        });
    }

    private String removeProjectIdFromSettingsJson(Path settingsJsonPath, long newVersion, String projectId) throws IOException {
        return updateSettingsJson(settingsJsonPath, newVersion, map -> {
            @SuppressWarnings("unchecked")
            final var projects = (Collection<String>) map.getOrDefault("projects", new HashSet<>());
            final var removed = projects.remove(projectId);
            if (removed == false) {
                logger.info("--> project [{}] does not exist in settings.json", projectId);
                return;
            }
            logger.info("--> removing project [{}] from settings.json", projectId);
            map.put("projects", projects);
        });
    }

    private String updateSettingsJson(Path settingsJsonPath, long newVersion, Consumer<Map<String, Object>> updater) throws IOException {
        final Map<String, Object> map;
        if (Files.exists(settingsJsonPath)) {
            map = XContentHelper.convertToMap(JSON.xContent(), Files.readString(settingsJsonPath), false);
        } else {
            map = XContentHelper.convertToMap(JSON.xContent(), """
                {
                     "metadata": {
                         "version": "0",
                         "compatibility": "8.4.0"
                     },
                     "state": {
                     },
                     "projects": []
                }""", false);
        }

        updater.accept(map);
        @SuppressWarnings("unchecked")
        final var metadata = (Map<String, Object>) map.get("metadata");
        assertNotNull(metadata);
        metadata.put("version", Strings.format("%s", newVersion));

        return XContentHelper.convertToJson(XContentTestUtils.convertToXContent(map, JSON), false, XContentType.JSON);
    }

    protected static ProjectLease readProjectLease(String blobName) throws IOException {
        try (var in = getCurrentMasterObjectStoreService().getClusterRootContainer().readBlob(OperationPurpose.CLUSTER_STATE, blobName)) {
            return ProjectLease.fromBytes(new BytesArray(in.readAllBytes()));
        }
    }
}
