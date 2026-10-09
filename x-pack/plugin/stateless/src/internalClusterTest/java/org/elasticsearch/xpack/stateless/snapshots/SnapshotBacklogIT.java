/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.RepositoryBacklog;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotBacklogIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class); // to block the reads of a node
        plugins.add(InternalSettingsPlugin.class); // for the setting that turns merging off
        return List.copyOf(plugins);
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    private static Settings.Builder snapshotNodeSettings() {
        return Settings.builder()
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(SnapshotBacklogTracker.BACKLOG_TRACKING_ENABLED_SETTING.getKey(), true)
            .put(SnapshotBacklogTracker.EVALUATION_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(200))
            // snapshots as in serverless, where the shard is read through the commit that the snapshots commit service holds
            .put(StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            // no background flushes, so that the commits are only the ones the test makes
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    private void createIndexWithOneShard(String indexName, Settings.Builder extraSettings) {
        createIndex(indexName, indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).put(extraSettings.build()).build());
        ensureGreen(indexName);
    }

    private void indexAndFlush(String indexName) {
        indexDocs(indexName, between(50, 100));
        flush(indexName);
    }

    /**
     * Waits until the backlog of the repository on the node is fully known, i.e. no shard is unknown, and has the given value.
     */
    private RepositoryBacklog awaitKnownBacklog(String node, ProjectRepo repo, long expectedBytes) throws Exception {
        final var backlog = new RepositoryBacklog[1];
        assertBusy(() -> {
            backlog[0] = getBacklog(node, repo);
            assertThat(backlog[0].unknownShards(), equalTo(0));
            assertThat(backlog[0].bytes(), equalTo(expectedBytes));
        });
        return backlog[0];
    }

    private long awaitPositiveKnownBacklog(String node, ProjectRepo repo) throws Exception {
        final var backlog = new RepositoryBacklog[1];
        assertBusy(() -> {
            backlog[0] = getBacklog(node, repo);
            assertThat(backlog[0].unknownShards(), equalTo(0));
            assertThat(backlog[0].bytes(), greaterThan(0L));
        });
        return backlog[0].bytes();
    }

    /**
     * @return the files of the latest commit of the only shard of the index, with their lengths
     */
    private Map<String, Long> getCommitFiles(String indexName) throws IOException {
        final var shard = findIndexShard(resolveIndex(indexName), 0);
        try (var commitRef = shard.acquireLastIndexCommit(false)) {
            return SnapshotBacklogTracker.getCommitFiles(commitRef.getIndexCommit());
        }
    }

    /**
     * @return the total length of the files of the commit that the other commit does not have
     */
    private static long newBytes(Map<String, Long> commitFiles, Map<String, Long> otherCommitFiles) {
        long bytes = 0;
        for (var commitFile : commitFiles.entrySet()) {
            if (commitFile.getValue().equals(otherCommitFiles.get(commitFile.getKey())) == false) {
                bytes += commitFile.getValue();
            }
        }
        return bytes;
    }

    /**
     * @return the total length of the files of the commit that a snapshot keeps in its shard-level metadata, which the tracker does not
     *         count as backlog, but may take off a little too much from the progress of a snapshot, see {@link ShardBacklog}
     */
    private static long inlinedBytes(Map<String, Long> commitFiles) {
        return commitFiles.entrySet()
            .stream()
            .filter(commitFile -> Store.MetadataSnapshot.isReadAsHash(commitFile.getKey()))
            .mapToLong(Map.Entry::getValue)
            .sum();
    }

    /**
     * Waits until the backlog of the repository on the node is fully known, i.e. no shard is unknown, and between the given values.
     */
    private void awaitKnownBacklogBetween(String node, ProjectRepo repo, long min, long max) throws Exception {
        assertBusy(() -> {
            final var backlog = getBacklog(node, repo);
            assertThat(backlog.unknownShards(), equalTo(0));
            assertThat(backlog.bytes(), allOf(greaterThanOrEqualTo(min), lessThanOrEqualTo(max)));
        });
    }

    /**
     * Watches the backlog of the repository on the node all the time, to see if it does something that a periodic check does not see
     * when it only does it for a moment. While a snapshot runs and after it is done the backlog only goes down, from what the shard had
     * to what it got after the snapshot's commit, and must not jump back to all that the shard has when the snapshot finishes, until the
     * node has caught up with what the snapshot made in the repository. Then, with a new commit, it must not be more than the files of
     * that commit.
     */
    private class BacklogSampler implements AutoCloseable {
        private final SnapshotBacklogTracker tracker;
        private final ProjectRepo repo;
        private final AtomicReference<String> violation = new AtomicReference<>();
        private final Thread thread;
        // the most that the backlog may be: what it was last while it only goes down, and then a given bound
        private long bound;
        private boolean onlyGoesDown = true;
        private volatile boolean running = true;

        BacklogSampler(String node, ProjectRepo repo, long backlog) {
            this.tracker = internalCluster().getInstance(SnapshotBacklogTracker.class, node);
            this.repo = repo;
            this.bound = backlog;
            this.thread = new Thread(this::sample, "backlog-sampler");
            thread.start();
        }

        /**
         * From now on the backlog may be at most the given bound
         */
        synchronized void setBound(long bound) {
            this.bound = bound;
            this.onlyGoesDown = false;
        }

        private void sample() {
            while (running) {
                final var backlog = tracker.getBacklog().get(repo);
                if (backlog != null && backlog.unknownShards() == 0) {
                    check(backlog);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }

        private synchronized void check(RepositoryBacklog backlog) {
            if (backlog.bytes() > bound) {
                violation.compareAndSet(null, "backlog " + backlog + " is above " + bound);
            } else if (onlyGoesDown) {
                bound = backlog.bytes();
            }
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            thread.join();
            assertThat(violation.get(), nullValue());
        }
    }

    private record AfterSnapshot(long newBytes, long inlinedBytes, long backlog) {}

    /**
     * Takes a snapshot of the only shard of the index, which has a backlog of the given size, and then gives it a new commit, and checks
     * that the backlog goes from what is left to upload while the snapshot runs to what the new commit has, and never to more.
     */
    private AfterSnapshot snapshotAndCommit(String node, ProjectRepo repo, String indexName, long backlog, Runnable snapshot)
        throws Exception {
        final var firstCommit = getCommitFiles(indexName);
        try (var sampler = new BacklogSampler(node, repo, backlog)) {
            snapshot.run();
            // the snapshot has uploaded everything, and the node may not know yet what the repository holds, but the backlog must not go up
            awaitKnownBacklog(node, repo, 0);

            sampler.setBound(Long.MAX_VALUE); // until the new commit is there
            indexAndFlush(indexName);
            final var secondCommit = getCommitFiles(indexName);
            final long newBytes = newBytes(secondCommit, firstCommit);
            sampler.setBound(newBytes);
            final long newBacklog = awaitPositiveKnownBacklog(node, repo);
            assertThat(newBacklog, lessThanOrEqualTo(newBytes));
            return new AfterSnapshot(newBytes, inlinedBytes(secondCommit), newBacklog);
        }
    }

    private RepositoryBacklog getBacklog(String node, ProjectRepo repo) {
        final var backlog = internalCluster().getInstance(SnapshotBacklogTracker.class, node).getBacklog().get(repo);
        assertThat(backlog, notNullValue());
        return backlog;
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory:DEBUG",
        reason = "to see which way the shard is read for a snapshot"
    )
    public void testBacklogFollowsSnapshotsAndTheirDeletion() throws Exception {
        final var node = startMasterAndIndexNode(snapshotNodeSettings().build());
        final var indexName = randomIdentifier();
        createIndexWithOneShard(indexName, Settings.builder());
        indexAndFlush(indexName);

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var repo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        // nothing is in the repository: everything has to be uploaded
        final long initialBacklog = awaitPositiveKnownBacklog(node, repo);
        assertThat(getBacklog(node, repo).countedShards(), equalTo(1));
        assertThat(getBacklog(node, repo).largestShardBytes(), equalTo(initialBacklog));

        // After a snapshot there is nothing left to upload, whichever way the shard is read for it. That is the way of serverless: the
        // commit info comes from the snapshots commit service, which logs it. A new commit is a backlog again, and it is only its files.
        snapshotAndCommit(
            node,
            repo,
            indexName,
            initialBacklog,
            () -> MockLog.assertThatLogger(
                () -> createSnapshot(repoName, "snap", List.of(indexName), List.of()),
                StatelessSnapshotShardContextFactory.class,
                new MockLog.SeenEventExpectation(
                    "the stateless snapshot path",
                    StatelessSnapshotShardContextFactory.class.getCanonicalName(),
                    Level.DEBUG,
                    "*acquiring commit info for snapshot*enabled status [ENABLED*"
                )
            )
        );

        // without the snapshot, the repository holds nothing again, and everything has to be uploaded
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").get());
        assertBusy(() -> {
            final var backlog = getBacklog(node, repo);
            assertThat(backlog.unknownShards(), equalTo(0));
            assertThat(backlog.bytes(), greaterThan(initialBacklog));
        });
    }

    public void testBacklogOfAShardThatMovedIsUnknownUntilItsFilesAreRead() throws Exception {
        startMasterOnlyNode(snapshotNodeSettings().build());
        final var sourceNode = startIndexNode(snapshotNodeSettings().build());
        final var targetNode = startIndexNode(snapshotNodeSettings().build());
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndexWithOneShard(indexName, Settings.builder().put("index.routing.allocation.exclude._name", targetNode));
        indexAndFlush(indexName);

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");
        final var repo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        final var afterSnapshot = snapshotAndCommit(
            sourceNode,
            repo,
            indexName,
            awaitPositiveKnownBacklog(sourceNode, repo),
            () -> createSnapshot(repoName, "snap", List.of(indexName), List.of())
        );

        // the target node cannot read anything from the repository for now
        final var targetRepository = (MockRepository) internalCluster().getInstance(RepositoriesService.class, targetNode)
            .repository(ProjectId.DEFAULT, repoName);
        targetRepository.setBlockOnAnyFiles();
        try {
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", sourceNode));
            ensureGreen(indexName);
            assertThat(internalCluster().nodesInclude(indexName), equalTo(Set.of(targetNode)));

            // the shard is on the target node, where it is not known what the repository holds of it: it is not counted, and not zero
            assertBusy(() -> {
                final var unknown = getBacklog(targetNode, repo);
                assertThat(unknown.unknownShards(), equalTo(1));
                assertThat(unknown.countedShards(), equalTo(0));
                assertThat(unknown.bytes(), equalTo(0L));
            });
            // and the source node does not report it any more
            assertBusy(() -> assertThat(getBacklog(sourceNode, repo), equalTo(new RepositoryBacklog(0, 0, 0, 0))));
        } finally {
            targetRepository.unblock();
        }

        // Once the target node has read it, the shard has the backlog it had before it moved. That was the new files, plus at most the
        // small files that the snapshot keeps in its shard-level metadata, while the source node did not have what the snapshot made.
        awaitKnownBacklogBetween(targetNode, repo, afterSnapshot.backlog() - afterSnapshot.inlinedBytes(), afterSnapshot.backlog());
    }

    public void testBacklogIsUnknownWhileTheMasterCannotTellTheShardGenerations() throws Exception {
        final var masterNode = startMasterOnlyNode(snapshotNodeSettings().build());
        final var node = startIndexNode(snapshotNodeSettings().build());
        ensureStableCluster(2);

        final var indexName = randomIdentifier();
        createIndexWithOneShard(indexName, Settings.builder());
        indexAndFlush(indexName);
        final var repoName = randomIdentifier();
        final var repo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        // a master that does not have the action yet, as during a rolling upgrade
        final var denied = new AtomicInteger();
        final var masterTransport = MockTransportService.getInstance(masterNode);
        masterTransport.addRequestHandlingBehavior(TransportGetShardGenerationsAction.NAME, (handler, request, channel, task) -> {
            denied.incrementAndGet();
            channel.sendResponse(new ActionNotFoundTransportException(TransportGetShardGenerationsAction.NAME));
        });
        createRepository(repoName, "fs");
        try {
            // every evaluation asks again, and the shard stays unknown, never zero
            assertBusy(() -> {
                final var backlog = getBacklog(node, repo);
                assertThat(denied.get(), greaterThan(1));
                assertThat(backlog, equalTo(new RepositoryBacklog(0, 0, 1, 0)));
            });
        } finally {
            masterTransport.clearAllRules();
        }

        // once the master has the action, the backlog is known
        awaitPositiveKnownBacklog(node, repo);
    }

    public void testBacklogIsKnownAfterTheMasterFailsOver() throws Exception {
        startMasterOnlyNode(snapshotNodeSettings().build());
        startMasterOnlyNode(snapshotNodeSettings().build());
        final var node = startIndexNode(snapshotNodeSettings().build());
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndexWithOneShard(indexName, Settings.builder());
        indexAndFlush(indexName);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var repo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        final var firstCommit = getCommitFiles(indexName);
        final long initialBacklog = awaitPositiveKnownBacklog(node, repo);
        createSnapshot(repoName, "snap", List.of(indexName), List.of());

        // The new master has not loaded the repository data yet. The index node forgets what it knows by restarting, before anything is
        // done with the repository, so that the first thing the new master does with it is to answer the index node
        shutdownMasterNodeGracefully();
        ensureStableCluster(2);
        internalCluster().restartNode(node);
        ensureStableCluster(2);
        ensureGreen(indexName);
        awaitKnownBacklog(node, repo, 0);

        indexAndFlush(indexName);
        assertThat(awaitPositiveKnownBacklog(node, repo), lessThanOrEqualTo(newBytes(getCommitFiles(indexName), firstCommit)));
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").get());
        assertBusy(() -> {
            final var backlog = getBacklog(node, repo);
            assertThat(backlog.unknownShards(), equalTo(0));
            assertThat(backlog.bytes(), greaterThan(initialBacklog));
        });
    }

    public void testTheMasterTellsTheShardGenerationsOfAllTheShardsAskedFor() {
        startMasterOnlyNode(snapshotNodeSettings().build());
        startIndexNode(snapshotNodeSettings().build());
        ensureStableCluster(2);

        final var index1 = randomIdentifier();
        final var index2 = randomIdentifier();
        final var absentIndex = randomIdentifier();
        for (var indexName : List.of(index1, index2)) {
            createIndex(indexName, indexSettings(2, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build());
            ensureGreen(indexName);
            indexAndFlush(indexName);
        }
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var projectRepo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        final var shards = List.of(
            new ShardId(resolveIndex(index1), 0),
            new ShardId(resolveIndex(index1), 1),
            new ShardId(resolveIndex(index2), 1),
            new ShardId(resolveIndex(index1), 2), // the index has no such shard
            new ShardId(new Index(absentIndex, "absent-uuid"), 0)
        );

        // the repository is empty
        final var empty = getShardGenerations(projectRepo, shards);
        assertThat(empty.getRepositoryGeneration(), equalTo(RepositoryData.EMPTY_REPO_GEN));
        assertThat(empty.getShardGenerations().keySet(), equalTo(Set.copyOf(shards)));
        assertTrue(empty.getShardGenerations().values().stream().allMatch(Objects::isNull));

        // one snapshot of one index
        createSnapshot(repoName, "snap", List.of(index1), List.of());
        final var repositoryData = getRepositoryData(projectRepo);
        final var response = getShardGenerations(projectRepo, shards);
        assertThat(response.getRepositoryGeneration(), equalTo(repositoryData.getGenId()));
        assertThat(response.getShardGenerations().keySet(), equalTo(Set.copyOf(shards)));
        final var indexId = repositoryData.resolveIndexId(index1);
        for (int shard = 0; shard < 2; shard++) {
            final var generation = repositoryData.shardGenerations().getShardGen(indexId, shard);
            assertThat(generation, notNullValue());
            assertThat(response.getShardGenerations().get(shards.get(shard)), equalTo(new RepositoryShardGeneration(indexId, generation)));
        }
        // the other index is not in the repository, and neither is a shard it does not have
        assertThat(response.getShardGenerations().get(shards.get(2)), nullValue());
        assertThat(response.getShardGenerations().get(shards.get(3)), nullValue());
        assertThat(response.getShardGenerations().get(shards.get(4)), nullValue());

        // a repository that does not exist
        final var missing = new ProjectRepo(ProjectId.DEFAULT, randomIdentifier());
        final var failure = safeAwaitFailure(
            GetShardGenerationsResponse.class,
            listener -> client().execute(
                TransportGetShardGenerationsAction.TYPE,
                new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, missing, shards),
                listener
            )
        );
        assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(RepositoryMissingException.class));
    }

    private GetShardGenerationsResponse getShardGenerations(ProjectRepo projectRepo, List<ShardId> shards) {
        return safeGet(
            client().execute(
                TransportGetShardGenerationsAction.TYPE,
                new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, projectRepo, shards)
            )
        );
    }

    private RepositoryData getRepositoryData(ProjectRepo projectRepo) {
        final var repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class)
            .repository(projectRepo.projectId(), projectRepo.name());
        return safeAwait(listener -> repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, listener));
    }

    public void testTheFileLengthsOfACommitAreTheSameInTheDirectoryAndInTheUploadedBlob() throws Exception {
        final var node = startMasterAndIndexNode(snapshotNodeSettings().build());
        final var indexName = randomIdentifier();
        createIndexWithOneShard(indexName, Settings.builder());
        indexAndFlush(indexName);
        indexAndFlush(indexName);

        // The backlog compares files by name and length against what the repository holds, which is what a snapshot of the blob
        // locations recorded, so the lengths in the directory have to be the same as the ones of the uploaded files
        final var shard = findIndexShard(resolveIndex(indexName), 0);
        final var commitService = internalCluster().getInstance(StatelessCommitService.class, node);
        assertBusy(() -> {
            try (var commitRef = shard.acquireLastIndexCommit(false)) {
                final var commit = commitRef.getIndexCommit();
                assertThat(commit.getFileNames(), not(empty()));
                for (String fileName : commit.getFileNames()) {
                    final var blobLocation = commitService.getBlobLocation(shard.shardId(), fileName);
                    assertThat(fileName, blobLocation, notNullValue());
                    assertThat(fileName, blobLocation.fileLength(), equalTo(commit.getDirectory().fileLength(fileName)));
                }
            }
        });
    }

    public void testBacklogOfAHollowShardIsKnown() throws Exception {
        startMasterOnlyNode(snapshotNodeSettings().build());
        final var hollowSettings = snapshotNodeSettings().put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.ZERO)
            .put(HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.ZERO)
            .build();
        final var nodeA = startIndexNode(hollowSettings);
        final var nodeB = startIndexNode(hollowSettings);
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndexWithOneShard(indexName, Settings.builder().put("index.routing.allocation.exclude._name", nodeB));
        indexAndFlush(indexName);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var repo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        // the repository has the first commit, and the shard has a backlog of a second one
        final var afterSnapshot = snapshotAndCommit(
            nodeA,
            repo,
            indexName,
            awaitPositiveKnownBacklog(nodeA, repo),
            () -> createSnapshot(repoName, "snap", List.of(indexName), List.of())
        );

        // the shard moves to the other node, where it is hollow, and has the same backlog as before
        hollowShards(indexName, 1, nodeA, nodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).getEngineOrNull(), instanceOf(HollowIndexEngine.class));
        awaitKnownBacklogBetween(nodeB, repo, afterSnapshot.backlog() - afterSnapshot.inlinedBytes(), afterSnapshot.backlog());
        assertThat(getBacklog(nodeB, repo).countedShards(), equalTo(1));
    }
}
