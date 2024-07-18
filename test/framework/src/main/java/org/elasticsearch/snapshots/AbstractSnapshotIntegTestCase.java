/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.SnapshotSortKey;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ResolvedRepositories;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.snapshots.SnapshotsService.NO_FEATURE_STATES_VALUE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public abstract class AbstractSnapshotIntegTestCase extends ESIntegTestCase {

    public static final String RANDOM_SNAPSHOT_NAME_PREFIX = "snap-";

    public static final String OLD_VERSION_SNAPSHOT_PREFIX = "old-version-snapshot-";

    protected static final int LARGE_POOL_SIZE = 10;

    // Large snapshot pool settings to set up nodes for tests involving multiple repositories that need to have enough
    // threads so that blocking some threads on one repository doesn't block other repositories from doing work
    protected static final Settings LARGE_SNAPSHOT_POOL_SETTINGS = Settings.builder()
        .put("thread_pool.snapshot.core", LARGE_POOL_SIZE)
        .put("thread_pool.snapshot.max", LARGE_POOL_SIZE)
        .build();

    protected static final Settings SMALL_SNAPSHOT_POOL_SETTINGS = Settings.builder()
        .put("thread_pool.snapshot.core", 1)
        .put("thread_pool.snapshot.max", 1)
        .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Rebalancing is causing some checks after restore to randomly fail
            // due to https://github.com/elastic/elasticsearch/issues/9421
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class);
    }

    @After
    public void assertConsistentHistoryInLuceneIndex() throws Exception {
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
    }

    @After
    public void verifyNoLeakedListeners() throws Exception {
        assertBusy(() -> {
            for (SnapshotsService snapshotsService : internalCluster().getInstances(SnapshotsService.class)) {
                assertTrue(snapshotsService.assertAllListenersResolved());
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private String skipRepoConsistencyCheckReason;

    @After
    public void assertRepoConsistency() {
        if (skipRepoConsistencyCheckReason == null) {
            clusterAdmin().prepareGetRepositories(TEST_REQUEST_TIMEOUT).get().repositories().forEach(repositoryMetadata -> {
                final String name = repositoryMetadata.name();
                if (repositoryMetadata.settings().getAsBoolean(READONLY_SETTING_KEY, false) == false) {
                    clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, name, OLD_VERSION_SNAPSHOT_PREFIX + "*").get();
                    clusterAdmin().prepareCleanupRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name).get();
                }
                BlobStoreTestUtil.assertConsistency(getRepositoryOnMaster(name));
            });
        } else {
            logger.info("--> skipped repo consistency checks because [{}]", skipRepoConsistencyCheckReason);
        }
    }

    protected void disableRepoConsistencyCheck(String reason) {
        assertNotNull(reason);
        skipRepoConsistencyCheckReason = reason;
    }

    protected RepositoryData getRepositoryData(String repoName, IndexVersion version) {
        final RepositoryData repositoryData = getRepositoryData(repoName);
        if (SnapshotsService.includesUUIDs(version) == false) {
            return repositoryData.withoutUUIDs();
        } else {
            return repositoryData;
        }
    }

    @SuppressWarnings("cast")
    protected RepositoryData getRepositoryData(String repository) {
        return getRepositoryData((Repository) getRepositoryOnMaster(repository));
    }

    public static RepositoryData getRepositoryData(Repository repository) {
        return safeAwait(listener -> repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, listener));
    }

    public static long getFailureCount(String repository) {
        long failureCount = 0;
        for (RepositoriesService repositoriesService : internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            failureCount += mockRepository.getFailureCount();
        }
        return failureCount;
    }

    public static void assertFileCount(Path dir, int expectedCount) throws IOException {
        final List<Path> found = getAllFilesInDirectoryAndDescendants(dir);
        assertEquals("Unexpected file count, found: [" + found + "].", expectedCount, found.size());
    }

    protected static List<Path> getAllFilesInDirectoryAndDescendants(Path dir) throws IOException {
        final List<Path> found = new ArrayList<>();
        forEachFileRecursively(dir, ((path, basicFileAttributes) -> found.add(path)));
        return found;
    }

    protected void stopNode(final String node) throws IOException {
        logger.info("--> stopping node {}", node);
        internalCluster().stopNode(node);
    }

    protected static String startDataNodeWithLargeSnapshotPool() {
        return internalCluster().startDataOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
    }

    public void waitForBlock(String node, String repository) throws Exception {
        logger.info("--> waiting for [{}] to be blocked on node [{}]", repository, node);
        MockRepository mockRepository = getRepositoryOnNode(repository, node);
        assertBusy(() -> assertTrue(mockRepository.blocked()), 30L, TimeUnit.SECONDS);
    }

    public static void blockMasterFromFinalizingSnapshotOnIndexFile(final String repositoryName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repositoryName).setBlockAndFailOnWriteIndexFile();
    }

    public static void blockMasterOnWriteIndexFile(final String repositoryName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repositoryName).setBlockOnWriteIndexFile();
    }

    public static void blockMasterFromDeletingIndexNFile(String repositoryName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repositoryName).setBlockOnDeleteIndexFile();
    }

    public static void blockMasterFromFinalizingSnapshotOnSnapFile(final String repositoryName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repositoryName).setBlockAndFailOnWriteSnapFiles();
    }

    public static void blockMasterOnAnyDataFile(final String repositoryName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repositoryName).blockOnDataFiles();
    }

    public static void blockMasterOnShardLevelSnapshotFile(final String repositoryName, String indexId) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnMaster(repositoryName).setBlockOnShardLevelSnapFiles(indexId);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Repository> T getRepositoryOnMaster(String repositoryName) {
        return ((T) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repositoryName));
    }

    @SuppressWarnings("unchecked")
    protected static <T extends Repository> T getRepositoryOnNode(String repositoryName, String nodeName) {
        return ((T) internalCluster().getInstance(RepositoriesService.class, nodeName).repository(repositoryName));
    }

    public static String blockNodeWithIndex(final String repositoryName, final String indexName) {
        for (String node : internalCluster().nodesInclude(indexName)) {
            AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnNode(repositoryName, node).blockOnDataFiles();
            return node;
        }
        fail("No nodes for the index " + indexName + " found");
        return null;
    }

    public static void blockNodeOnAnyFiles(String repository, String nodeName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnNode(repository, nodeName).setBlockOnAnyFiles();
    }

    public static void blockDataNode(String repository, String nodeName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnNode(repository, nodeName).blockOnDataFiles();
    }

    public static void blockAndFailDataNode(String repository, String nodeName) {
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnNode(repository, nodeName).blockAndFailOnDataFiles();
    }

    public static void blockAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repository)).blockOnDataFiles();
        }
    }

    public static void unblockAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repository)).unblock();
        }
    }

    public static void failReadsAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            mockRepository.setFailReadsAfterUnblock(true);
        }
    }

    public static void waitForBlockOnAnyDataNode(String repository) throws InterruptedException {
        final boolean blocked = waitUntil(() -> {
            for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
                MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
                if (mockRepository.blocked()) {
                    return true;
                }
            }
            return false;
        }, 30L, TimeUnit.SECONDS);

        assertTrue("No repository is blocked waiting on a data node", blocked);
    }

    public void unblockNode(final String repository, final String node) {
        logger.info("--> unblocking [{}] on node [{}]", repository, node);
        AbstractSnapshotIntegTestCase.<MockRepository>getRepositoryOnNode(repository, node).unblock();
    }

    protected void createRepository(String repoName, String type, Settings.Builder settings, boolean verify) {
        createRepository(logger, repoName, type, settings, verify);
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

    protected void createRepository(String repoName, String type, Settings.Builder settings) {
        createRepository(repoName, type, settings, true);
    }

    protected void createRepository(String repoName, String type, Path location) {
        createRepository(repoName, type, Settings.builder().put("location", location));
    }

    protected void createRepository(String repoName, String type) {
        createRepository(logger, repoName, type);
    }

    protected void createRepositoryNoVerify(String repoName, String type) {
        createRepository(repoName, type, randomRepositorySettings(), false);
    }

    public static void createRepository(Logger logger, String repoName, String type) {
        createRepository(logger, repoName, type, randomRepositorySettings(), true);
    }

    protected void deleteRepository(String repoName) {
        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName));
    }

    public static Settings.Builder randomRepositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", randomRepoPath()).put("compress", randomBoolean());
        if (rarely()) {
            settings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
        }
        if (randomBoolean()) {
            settings.put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), randomBoolean());
        }
        return settings;
    }

    protected static Settings.Builder indexSettingsNoReplicas(int shards) {
        return indexSettings(shards, 0);
    }

    /**
     * Randomly write an empty snapshot of an older version to an empty repository to simulate an older repository metadata format.
     */
    protected void maybeInitWithOldSnapshotVersion(String repoName, Path repoPath) throws Exception {
        if (randomBoolean() && randomBoolean()) {
            initWithSnapshotVersion(
                repoName,
                repoPath,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_7_0_0, IndexVersions.V_8_9_0)
            );
        }
    }

    /**
     * Workaround to simulate BwC situation: taking a snapshot without indices here so that we don't create any new version shard
     * generations (the existence of which would short-circuit checks for the repo containing old version snapshots)
     */
    protected String initWithSnapshotVersion(String repoName, Path repoPath, IndexVersion version) throws Exception {
        assertThat("This hack only works on an empty repository", getRepositoryData(repoName).getSnapshotIds(), empty());
        final String oldVersionSnapshot = OLD_VERSION_SNAPSHOT_PREFIX + version.id();
        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            repoName,
            oldVersionSnapshot
        ).setIndices("does-not-exist-for-sure-*").setWaitForCompletion(true).get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.totalShards(), is(0));

        logger.info("--> writing downgraded RepositoryData for repository metadata version [{}]", version);
        final RepositoryData repositoryData = getRepositoryData(repoName, version);
        final XContentBuilder jsonBuilder = JsonXContent.contentBuilder();
        repositoryData.snapshotsToXContent(jsonBuilder, version);
        final var currentVersionString = Strings.toString(jsonBuilder);
        final String oldVersionString;
        if (version.onOrAfter(IndexVersions.FIRST_DETACHED_INDEX_VERSION)) {
            oldVersionString = currentVersionString.replace(
                ",\"index_version\":" + IndexVersion.current(),
                ",\"index_version\":" + version
            );
        } else {
            oldVersionString = currentVersionString.replace(",\"index_version\":" + IndexVersion.current(), "")
                .replace(",\"version\":\"8.11.0\"", ",\"version\":\"" + Version.fromId(version.id()) + "\"");
        }
        final RepositoryData downgradedRepoData;
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, oldVersionString)) {
            downgradedRepoData = RepositoryData.snapshotsFromXContent(parser, repositoryData.getGenId(), randomBoolean());
        }
        Files.write(
            repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + repositoryData.getGenId()),
            BytesReference.toBytes(BytesReference.bytes(downgradedRepoData.snapshotsToXContent(XContentFactory.jsonBuilder(), version))),
            StandardOpenOption.TRUNCATE_EXISTING
        );
        final SnapshotInfo downgradedSnapshotInfo;
        try (
            var parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                Strings.toString(snapshotInfo, ChecksumBlobStoreFormat.SNAPSHOT_ONLY_FORMAT_PARAMS)
                    .replace(IndexVersion.current().toString(), version.toString())
            )
        ) {
            downgradedSnapshotInfo = SnapshotInfo.fromXContentInternal(repoName, parser);
        }
        final BlobStoreRepository blobStoreRepository = getRepositoryOnMaster(repoName);
        BlobStoreRepository.SNAPSHOT_FORMAT.write(
            downgradedSnapshotInfo,
            blobStoreRepository.blobStore().blobContainer(blobStoreRepository.basePath()),
            snapshotInfo.snapshotId().getUUID(),
            randomBoolean()
        );

        final RepositoryMetadata repoMetadata = blobStoreRepository.getMetadata();
        if (BlobStoreRepository.CACHE_REPOSITORY_DATA.get(repoMetadata.settings())) {
            logger.info("--> recreating repository to clear caches");
            assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName));
            createRepository(repoName, repoMetadata.type(), Settings.builder().put(repoMetadata.settings()));
        }
        return oldVersionSnapshot;
    }

    protected SnapshotInfo createFullSnapshot(String repoName, String snapshotName) {
        return createFullSnapshot(logger, repoName, snapshotName);
    }

    public static SnapshotInfo createFullSnapshot(Logger logger, String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] in [{}]", snapshotName, repoName);
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        return snapshotInfo;
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

    protected SnapshotInfo createSnapshot(String repositoryName, String snapshot, List<String> indices) {
        return createSnapshot(repositoryName, snapshot, indices, Collections.singletonList(NO_FEATURE_STATES_VALUE));
    }

    protected void createIndexWithRandomDocs(String indexName, int docCount) throws InterruptedException {
        createIndex(indexName);
        ensureGreen();
        indexRandomDocs(indexName, docCount);
    }

    protected void indexRandomDocs(String index, int numdocs) throws InterruptedException {
        logger.info("--> indexing [{}] documents into [{}]", numdocs, index);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(index).setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh(index);
        assertDocCount(index, numdocs);
    }

    protected long getCountForIndex(String indexName) {
        return SearchResponseUtils.getTotalHitsValue(
            prepareSearch(indexName).setSource(new SearchSourceBuilder().size(0).trackTotalHits(true))
        );
    }

    protected void assertDocCount(String index, long count) {
        assertEquals(getCountForIndex(index), count);
    }

    /**
     * Adds a snapshot in state {@link SnapshotState#FAILED} to the given repository.
     *
     * @param repoName     repository to add snapshot to
     * @param snapshotName name for the new failed snapshot
     * @param metadata     snapshot metadata to write (as returned by {@link SnapshotInfo#userMetadata()})
     */
    protected void addBwCFailedSnapshot(String repoName, String snapshotName, Map<String, Object> metadata) throws Exception {
        final ClusterState state = clusterAdmin().prepareState().get().getState();
        final RepositoriesMetadata repositoriesMetadata = state.metadata().custom(RepositoriesMetadata.TYPE);
        assertNotNull(repositoriesMetadata);
        final RepositoryMetadata initialRepoMetadata = repositoriesMetadata.repository(repoName);
        assertNotNull(initialRepoMetadata);
        assertThat(
            "We can only manually insert a snapshot into a repository that does not have a generation tracked in the CS",
            initialRepoMetadata.generation(),
            is(RepositoryData.UNKNOWN_REPO_GEN)
        );
        final Repository repo = getRepositoryOnMaster(repoName);
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID(random()));
        logger.info("--> adding old version FAILED snapshot [{}] to repository [{}]", snapshotId, repoName);
        final SnapshotInfo snapshotInfo = new SnapshotInfo(
            new Snapshot(repoName, snapshotId),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            "failed on purpose",
            SnapshotsService.OLD_SNAPSHOT_FORMAT,
            0L,
            0L,
            0,
            0,
            Collections.emptyList(),
            randomBoolean(),
            metadata,
            SnapshotState.FAILED,
            Collections.emptyMap()
        );
        safeAwait(
            (ActionListener<RepositoryData> listener) -> repo.finalizeSnapshot(
                new FinalizeSnapshotContext(
                    ShardGenerations.EMPTY,
                    getRepositoryData(repoName).getGenId(),
                    state.metadata(),
                    snapshotInfo,
                    SnapshotsService.OLD_SNAPSHOT_FORMAT,
                    listener,
                    info -> {}
                )
            )
        );
    }

    protected void awaitNDeletionsInProgress(int count) throws Exception {
        logger.info("--> wait for [{}] deletions to show up in the cluster state", count);
        awaitClusterState(state -> SnapshotDeletionsInProgress.get(state).getEntries().size() == count);
    }

    protected void awaitNoMoreRunningOperations() throws Exception {
        awaitNoMoreRunningOperations(internalCluster().getMasterName());
    }

    protected void awaitNoMoreRunningOperations(String viaNode) throws Exception {
        logger.info("--> verify no more operations in the cluster state");
        awaitClusterState(
            logger,
            viaNode,
            state -> SnapshotsInProgress.get(state).isEmpty() && SnapshotDeletionsInProgress.get(state).hasDeletionsInProgress() == false
        );
    }

    protected ActionFuture<CreateSnapshotResponse> startFullSnapshotBlockedOnDataNode(String snapshotName, String repoName, String dataNode)
        throws Exception {
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> fut = startFullSnapshot(repoName, snapshotName);
        waitForBlock(dataNode, repoName);
        return fut;
    }

    protected ActionFuture<CreateSnapshotResponse> startFullSnapshot(String repoName, String snapshotName) {
        return startFullSnapshot(repoName, snapshotName, false);
    }

    protected ActionFuture<CreateSnapshotResponse> startFullSnapshot(String repoName, String snapshotName, boolean partial) {
        return startFullSnapshot(logger, repoName, snapshotName, partial);
    }

    public static ActionFuture<CreateSnapshotResponse> startFullSnapshot(
        Logger logger,
        String repoName,
        String snapshotName,
        boolean partial
    ) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        return clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setWaitForCompletion(true)
            .setPartial(partial)
            .execute();
    }

    protected void awaitNumberOfSnapshotsInProgress(int count) throws Exception {
        awaitNumberOfSnapshotsInProgress(logger, count);
    }

    public static void awaitNumberOfSnapshotsInProgress(Logger logger, int count) throws Exception {
        logger.info("--> wait for [{}] snapshots to show up in the cluster state", count);
        awaitClusterState(logger, state -> SnapshotsInProgress.get(state).count() == count);
    }

    protected SnapshotInfo assertSuccessful(ActionFuture<CreateSnapshotResponse> future) throws Exception {
        return assertSuccessful(logger, future);
    }

    public static SnapshotInfo assertSuccessful(Logger logger, ActionFuture<CreateSnapshotResponse> future) throws Exception {
        logger.info("--> wait for snapshot to finish");
        final SnapshotInfo snapshotInfo = future.get().getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        return snapshotInfo;
    }

    public static final Settings SINGLE_SHARD_NO_REPLICA = indexSettingsNoReplicas(1).build();

    protected void createIndexWithContent(String indexName) {
        createIndexWithContent(indexName, SINGLE_SHARD_NO_REPLICA);
    }

    protected void createIndexWithContent(String indexName, Settings indexSettings) {
        logger.info("--> creating index [{}]", indexName);
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);
        indexDoc(indexName, "some_id", "foo", "bar");
    }

    protected ActionFuture<AcknowledgedResponse> startDeleteSnapshot(String repoName, String snapshotName) {
        logger.info("--> deleting snapshot [{}] from repo [{}]", snapshotName, repoName);
        return clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName).execute();
    }

    protected ActionFuture<AcknowledgedResponse> startDeleteSnapshots(String repoName, List<String> snapshotNames, String viaNode) {
        logger.info("--> deleting snapshots {} from repo [{}]", snapshotNames, repoName);
        return client(viaNode).admin()
            .cluster()
            .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotNames.toArray(Strings.EMPTY_ARRAY))
            .execute();
    }

    protected static void updateClusterState(final Function<ClusterState, ClusterState> updater) throws Exception {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updater.apply(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                future.onResponse(null);
            }
        });
        future.get();
    }

    protected SnapshotInfo getSnapshot(String repository, String snapshot) {
        final List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repository)
            .setSnapshots(snapshot)
            .get()
            .getSnapshots();
        assertThat(snapshotInfos, hasSize(1));
        return snapshotInfos.get(0);
    }

    protected static ThreadPoolStats.Stats snapshotThreadPoolStats(final String node) {
        return StreamSupport.stream(internalCluster().getInstance(ThreadPool.class, node).stats().spliterator(), false)
            .filter(threadPool -> threadPool.name().equals(ThreadPool.Names.SNAPSHOT))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Failed to find snapshot pool on node [" + node + "]"));
    }

    protected void awaitMasterFinishRepoOperations() throws Exception {
        logger.info("--> waiting for master to finish all repo operations on its SNAPSHOT pool");
        final String masterName = internalCluster().getMasterName();
        assertBusy(() -> assertEquals(snapshotThreadPoolStats(masterName).active(), 0));
    }

    protected List<String> createNSnapshots(String repoName, int count) throws Exception {
        return createNSnapshots(logger, repoName, count);
    }

    public static List<String> createNSnapshots(Logger logger, String repoName, int count) throws Exception {
        final PlainActionFuture<Collection<CreateSnapshotResponse>> allSnapshotsDone = new PlainActionFuture<>();
        final ActionListener<CreateSnapshotResponse> snapshotsListener = new GroupedActionListener<>(count, allSnapshotsDone);
        final List<String> snapshotNames = new ArrayList<>(count);
        final String prefix = RANDOM_SNAPSHOT_NAME_PREFIX + UUIDs.randomBase64UUID(random()).toLowerCase(Locale.ROOT) + "-";
        for (int i = 0; i < count; i++) {
            final String snapshot = prefix + i;
            snapshotNames.add(snapshot);
            final Map<String, Object> userMetadata = randomUserMetadata();
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshot)
                .setWaitForCompletion(true)
                .setUserMetadata(userMetadata)
                .execute(snapshotsListener.delegateFailure((l, response) -> {
                    final SnapshotInfo snapshotInfoInResponse = response.getSnapshotInfo();
                    assertEquals(userMetadata, snapshotInfoInResponse.userMetadata());
                    clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName)
                        .setSnapshots(snapshot)
                        .execute(l.safeMap(getResponse -> {
                            assertEquals(snapshotInfoInResponse, getResponse.getSnapshots().get(0));
                            return response;
                        }));
                }));
        }
        for (CreateSnapshotResponse snapshotResponse : allSnapshotsDone.get()) {
            assertThat(snapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        }
        logger.info("--> created {} in [{}]", snapshotNames, repoName);
        return snapshotNames;
    }

    public static void forEachFileRecursively(Path path, CheckedBiConsumer<Path, BasicFileAttributes, IOException> forEach)
        throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                forEach.accept(file, attrs);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void assertSnapshotListSorted(List<SnapshotInfo> snapshotInfos, @Nullable SnapshotSortKey sort, SortOrder sortOrder) {
        final BiConsumer<SnapshotInfo, SnapshotInfo> assertion;
        if (sort == null) {
            assertion = (s1, s2) -> assertThat(s2, greaterThanOrEqualTo(s1));
        } else {
            assertion = switch (sort) {
                case START_TIME -> (s1, s2) -> assertThat(s2.startTime(), greaterThanOrEqualTo(s1.startTime()));
                case NAME -> (s1, s2) -> assertThat(s2.snapshotId().getName(), greaterThanOrEqualTo(s1.snapshotId().getName()));
                case DURATION -> (s1, s2) -> assertThat(s2.endTime() - s2.startTime(), greaterThanOrEqualTo(s1.endTime() - s1.startTime()));
                case INDICES -> (s1, s2) -> assertThat(s2.indices().size(), greaterThanOrEqualTo(s1.indices().size()));
                case SHARDS -> (s1, s2) -> assertThat(s2.totalShards(), greaterThanOrEqualTo(s1.totalShards()));
                case FAILED_SHARDS -> (s1, s2) -> assertThat(s2.failedShards(), greaterThanOrEqualTo(s1.failedShards()));
                case REPOSITORY -> (s1, s2) -> assertThat(s2.repository(), greaterThanOrEqualTo(s1.repository()));
            };
        }
        final BiConsumer<SnapshotInfo, SnapshotInfo> orderAssertion;
        if (sortOrder == SortOrder.ASC) {
            orderAssertion = assertion;
        } else {
            orderAssertion = (s1, s2) -> assertion.accept(s2, s1);
        }
        for (int i = 0; i < snapshotInfos.size() - 1; i++) {
            orderAssertion.accept(snapshotInfos.get(i), snapshotInfos.get(i + 1));
        }
    }

    /**
     * Randomly either generates some random snapshot user metadata or returns {@code null}.
     *
     * @return random snapshot user metadata or {@code null}
     */
    @Nullable
    public static Map<String, Object> randomUserMetadata() {
        if (randomBoolean()) {
            return null;
        }

        Map<String, Object> metadata = new HashMap<>();
        long fields = randomLongBetween(0, 4);
        for (int i = 0; i < fields; i++) {
            if (randomBoolean()) {
                metadata.put(
                    randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                    randomAlphaOfLengthBetween(5, 5)
                );
            } else {
                Map<String, Object> nested = new HashMap<>();
                long nestedFields = randomLongBetween(0, 4);
                for (int j = 0; j < nestedFields; j++) {
                    nested.put(
                        randomValueOtherThanMany(nested::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                        randomAlphaOfLengthBetween(5, 5)
                    );
                }
                metadata.put(randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)), nested);
            }
        }
        return metadata;
    }

    public static String[] matchAllPattern() {
        return randomBoolean() ? new String[] { "*" } : new String[] { ResolvedRepositories.ALL_PATTERN };
    }

    public RepositoryMetadata getRepositoryMetadata(String repo) {
        Optional<RepositoryMetadata> repositoryMetadata = RepositoriesMetadata.get(clusterService().state())
            .repositories()
            .stream()
            .filter(x -> x.name().equals(repo))
            .findFirst();
        assertTrue(repositoryMetadata.isPresent());
        return repositoryMetadata.get();
    }
}
