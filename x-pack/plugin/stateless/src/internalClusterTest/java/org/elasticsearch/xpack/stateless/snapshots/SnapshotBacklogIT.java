/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepository;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.RepositoryBacklog;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

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
