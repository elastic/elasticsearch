/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.METADATA_BLOB_NAME_SUFFIX;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.METADATA_PREFIX;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks which indices a snapshot deletion rewrites, and which index-metadata blobs it leaves behind.
 * <p>
 * An index is rewritten only when the deletion removes some but not all of its snapshots. Fully deleted indices are removed with their
 * blob container, and fully untouched indices are skipped. Index-metadata blobs that no surviving snapshot of the index still references
 * are removed; a blob shared with a surviving snapshot is kept.
 */
public class SnapshotDeletionIndexSelectionTests extends ESSingleNodeTestCase {

    private static final String REPO_TYPE = "tracking-fs";
    private static final String REPO_NAME = "test-repo";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TrackingFsRepositoryPlugin.class);
    }

    @After
    public void deleteRepository() {
        for (var repository : getInstanceFromNode(RepositoriesService.class).getProjectRepositories(ProjectId.DEFAULT).keySet()) {
            assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repository).get());
        }
    }

    public void testDeletingOneSnapshotSelectsMixedIndicesAndCleansMetadata() throws IOException {
        createIndices("only-kept", "shared-metadata", "replaced-metadata", "only-deleted");

        final var repoPath = createRepository();
        createSnapshot("snap-kept", "only-kept", "shared-metadata", "replaced-metadata");

        // A new mapping gives replaced-metadata its own metadata blob. shared-metadata is unchanged, so the two snapshots share its blob.
        assertAcked(indicesAdmin().preparePutMapping("replaced-metadata").setSource("new_field", "type=keyword"));
        final var snapDeleted = createSnapshot("snap-deleted", "only-deleted", "shared-metadata", "replaced-metadata");

        final var repositoryDataBefore = getRepositoryData();
        assertIndexMetadataBlobsMatchRepositoryData(repoPath, repositoryDataBefore);

        deleteSnapshots("snap-deleted");

        // only-kept is absent from the deleted snapshot, only-deleted loses its only snapshot, and the other two lose one of two.
        assertTouchedIndices(repositoryDataBefore, Set.of("shared-metadata", "replaced-metadata"));
        final var repositoryDataAfter = getRepositoryData();
        assertIndexMetadataBlobsMatchRepositoryData(repoPath, repositoryDataAfter);
        assertRemovedIndicesDeleted(repoPath, repositoryDataBefore, repositoryDataAfter);
    }

    public void testDeletingSeveralSnapshotsSelectsMixedIndicesAndCleansMetadata() throws IOException {
        createIndices("only-a", "only-b", "only-c", "both-deleted", "shared-metadata", "replaced-metadata");

        final var repoPath = createRepository();
        final var snapA = createSnapshot("snap-a", "only-a", "both-deleted", "shared-metadata", "replaced-metadata");
        final var snapB = createSnapshot("snap-b", "only-b", "both-deleted", "shared-metadata");

        assertAcked(indicesAdmin().preparePutMapping("replaced-metadata").setSource("new_field", "type=keyword"));
        createSnapshot("snap-c", "only-c", "shared-metadata", "replaced-metadata");

        final var repositoryDataBefore = getRepositoryData();
        assertIndexMetadataBlobsMatchRepositoryData(repoPath, repositoryDataBefore);

        deleteSnapshots("snap-a", "snap-b");

        // both-deleted loses every snapshot, only-c loses none, and shared-metadata and replaced-metadata lose some but not all.
        assertTouchedIndices(repositoryDataBefore, Set.of("shared-metadata", "replaced-metadata"));
        final var repositoryDataAfter = getRepositoryData();
        assertIndexMetadataBlobsMatchRepositoryData(repoPath, repositoryDataAfter);
        assertRemovedIndicesDeleted(repoPath, repositoryDataBefore, repositoryDataAfter);
    }

    private Path createRepository() {
        final var repoPath = ESIntegTestCase.randomRepoPath(node().settings());
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME)
                .setType(REPO_TYPE)
                .setSettings(Settings.builder().put("location", repoPath))
        );
        return repoPath;
    }

    private void createIndices(String... indices) {
        for (final var index : indices) {
            createIndex(index, indexSettings(1, 0).build());
            prepareIndex(index).setId("1").setSource(randomIdentifier("field-"), randomAlphaOfLengthBetween(1, 10)).get();
        }
    }

    private SnapshotId createSnapshot(String name, String... indices) {
        final var response = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, REPO_NAME, name)
            .setWaitForCompletion(true)
            .setIndices(indices)
            .get();
        assertThat(response.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(response.getSnapshotInfo().shardFailures(), empty());
        return response.getSnapshotInfo().snapshotId();
    }

    private void deleteSnapshots(String... names) {
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, REPO_NAME, names).get());
    }

    private TrackingRepository getTrackingRepository() {
        return asInstanceOf(
            TrackingRepository.class,
            getInstanceFromNode(RepositoriesService.class).repository(ProjectId.DEFAULT, REPO_NAME)
        );
    }

    private RepositoryData getRepositoryData() {
        return safeAwait(l -> getTrackingRepository().getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, l));
    }

    private void assertTouchedIndices(RepositoryData repositoryData, Set<String> expectedIndexNames) {
        final var namesByUuid = repositoryData.getIndices().values().stream().collect(Collectors.toMap(IndexId::getId, IndexId::getName));
        assertThat(
            getTrackingRepository().touchedIndexIds.stream()
                .map(indexUuid -> Objects.requireNonNull(namesByUuid.get(indexUuid)))
                .collect(Collectors.toSet()),
            equalTo(expectedIndexNames)
        );
    }

    private static void assertRemovedIndicesDeleted(Path repoPath, RepositoryData before, RepositoryData after) {
        for (final var indexId : before.getIndices().values()) {
            if (after.getIndices().containsKey(indexId.getName()) == false) {
                assertFalse(
                    "index [" + indexId.getName() + "] is no longer referenced but its repository directory remains",
                    Files.exists(repoPath.resolve("indices").resolve(indexId.getId()))
                );
            }
        }
    }

    private static void assertIndexMetadataBlobsMatchRepositoryData(Path repoPath, RepositoryData repositoryData) throws IOException {
        for (final var indexId : repositoryData.getIndices().values()) {
            try (
                var stream = Files.newDirectoryStream(
                    repoPath.resolve("indices").resolve(indexId.getId()),
                    METADATA_PREFIX + "*" + METADATA_BLOB_NAME_SUFFIX
                )
            ) {
                assertThat(
                    indexId.getName(),
                    StreamSupport.stream(stream.spliterator(), false).map(p -> p.getFileName().toString()).collect(Collectors.toSet()),
                    equalTo(
                        repositoryData.getSnapshots(indexId)
                            .stream()
                            .map(
                                snapshotId -> METADATA_PREFIX + repositoryData.indexMetaDataGenerations()
                                    .indexMetaBlobId(snapshotId, indexId) + METADATA_BLOB_NAME_SUFFIX
                            )
                            .collect(Collectors.toSet())
                    )
                );
            }
        }
    }

    /**
     * An {@code fs} repository that records index UUIDs whose blobs are read. Deleting a fully removed index's container does not count:
     * that cleanup walks the directory directly and does not read any of the blobs therein.
     */
    public static class TrackingFsRepositoryPlugin extends Plugin implements RepositoryPlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            return Collections.singletonMap(
                REPO_TYPE,
                (projectId, metadata) -> new TrackingRepository(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                )
            );
        }
    }

    private static class TrackingRepository extends FsRepository {

        /// Index UUIDs whose blobs were read during a deletion.
        final Set<String> touchedIndexIds = ConcurrentCollections.newConcurrentSet();

        TrackingRepository(
            ProjectId projectId,
            RepositoryMetadata metadata,
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(projectId, metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new TrackingBlobStore(super.createBlobStore());
        }

        @Override
        public void deleteSnapshots(
            Set<SnapshotId> snapshotIds,
            long repositoryDataGeneration,
            IndexVersion minimumNodeVersion,
            ActionListener<RepositoryData> repositoryDataUpdateListener,
            Runnable onCompletion
        ) {
            touchedIndexIds.clear();
            super.deleteSnapshots(snapshotIds, repositoryDataGeneration, minimumNodeVersion, repositoryDataUpdateListener, onCompletion);
        }

        private class TrackingBlobStore implements BlobStore {
            private final BlobStore delegate;

            private TrackingBlobStore(BlobStore delegate) {
                this.delegate = delegate;
            }

            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new TrackingBlobContainer(delegate.blobContainer(path));
            }

            @Override
            public void close() throws IOException {
                delegate.close();
            }
        }

        private class TrackingBlobContainer extends FilterBlobContainer {
            TrackingBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new TrackingBlobContainer(child);
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                final var parts = path().parts();
                if (parts.size() >= 2 && parts.get(0).equals("indices")) {
                    touchedIndexIds.add(parts.get(1));
                }
                return super.readBlob(purpose, blobName);
            }
        }
    }
}
