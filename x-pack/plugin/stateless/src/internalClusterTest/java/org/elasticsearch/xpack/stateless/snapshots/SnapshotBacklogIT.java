/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepository;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.RepositoryBacklog;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotBacklogIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
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

    private RepositoryBacklog getBacklog(String node, ProjectRepo repo) {
        final var backlog = internalCluster().getInstance(SnapshotBacklogTracker.class, node).getBacklog().get(repo);
        assertThat(backlog, notNullValue());
        return backlog;
    }

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

        // after a snapshot there is nothing left to upload
        createSnapshot(repoName, "snap", List.of(indexName), List.of());
        awaitKnownBacklog(node, repo, 0);

        // new data in a new commit is a backlog again, and it is only the new files
        indexAndFlush(indexName);
        final long newBacklog = awaitPositiveKnownBacklog(node, repo);

        // without the snapshot, the repository holds nothing again
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").get());
        awaitKnownBacklog(node, repo, initialBacklog + newBacklog);
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
        createRepository(repoName, StatelessMockRepositoryPlugin.TYPE);
        final var repo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        createSnapshot(repoName, "snap", List.of(indexName), List.of());
        indexAndFlush(indexName);
        final long backlog = awaitPositiveKnownBacklog(sourceNode, repo);

        // the target node cannot read anything from the repository for now
        final var blockedReads = new BlockMetadataReads();
        try {
            ((StatelessMockRepository) internalCluster().getInstance(RepositoriesService.class, targetNode)
                .repository(ProjectId.DEFAULT, repoName)).setStrategy(blockedReads);

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
            assertThat(getBacklog(sourceNode, repo), equalTo(new RepositoryBacklog(0, 0, 0, 0)));
        } finally {
            blockedReads.proceed.countDown();
        }

        // once the target node has read it, the shard has the backlog it had before it moved
        awaitKnownBacklog(targetNode, repo, backlog);
    }

    public void testBacklogIsUnknownWhileTheMasterCannotTellTheShardGenerations() throws Exception {
        final var masterNode = startMasterOnlyNode(snapshotNodeSettings().build());
        final var node = startIndexNode(snapshotNodeSettings().build());
        ensureStableCluster(2);

        final var indexName = randomIdentifier();
        createIndexWithOneShard(indexName, Settings.builder());
        indexAndFlush(indexName);
        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var repo = new ProjectRepo(ProjectId.DEFAULT, repoName);

        // a master that does not have the action yet, as during a rolling upgrade
        final var denied = new AtomicInteger();
        final var masterTransport = MockTransportService.getInstance(masterNode);
        masterTransport.addRequestHandlingBehavior(TransportGetShardGenerationsAction.NAME, (handler, request, channel, task) -> {
            denied.incrementAndGet();
            channel.sendResponse(new ActionNotFoundTransportException(TransportGetShardGenerationsAction.NAME));
        });
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

        final long initialBacklog = awaitPositiveKnownBacklog(node, repo);
        createSnapshot(repoName, "snap", List.of(indexName), List.of());
        awaitKnownBacklog(node, repo, 0);

        // the new master has not loaded the repository data yet, and answers from what it reads
        shutdownMasterNodeGracefully();
        ensureStableCluster(2);
        awaitKnownBacklog(node, repo, 0);

        indexAndFlush(indexName);
        final long newBacklog = awaitPositiveKnownBacklog(node, repo);
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").get());
        awaitKnownBacklog(node, repo, initialBacklog + newBacklog);
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
                new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, missing, RepositoryData.EMPTY_REPO_GEN, shards),
                listener
            )
        );
        assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(RepositoryMissingException.class));
    }

    private GetShardGenerationsResponse getShardGenerations(ProjectRepo projectRepo, List<ShardId> shards) {
        return safeGet(
            client().execute(
                TransportGetShardGenerationsAction.TYPE,
                new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, projectRepo, RepositoryData.EMPTY_REPO_GEN, shards)
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

        // the shard moves to the other node, where it is hollow
        hollowShards(indexName, 1, nodeA, nodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).getEngineOrNull(), instanceOf(HollowIndexEngine.class));

        // it has a backlog like any other shard, and is not unknown
        awaitPositiveKnownBacklog(nodeB, repo);
        assertThat(getBacklog(nodeB, repo).countedShards(), equalTo(1));
    }

    private static class BlockMetadataReads extends StatelessMockRepositoryStrategy {
        final CountDownLatch proceed = new CountDownLatch(1);

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName
        ) throws IOException {
            maybeBlock(purpose);
            return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName,
            long position,
            long length
        ) throws IOException {
            maybeBlock(purpose);
            return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
        }

        private void maybeBlock(OperationPurpose purpose) {
            if (purpose == OperationPurpose.SNAPSHOT_METADATA) {
                safeAwait(proceed);
            }
        }
    }
}
