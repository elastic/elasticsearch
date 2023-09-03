/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.util.SameThreadExecutorService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class BlobStoreTestUtil {

    /**
     * Assert that there are no unreferenced indices or unreferenced root-level metadata blobs in any repository.
     * TODO: Expand the logic here to also check for unreferenced segment blobs and shard level metadata
     * @param repository BlobStoreRepository to check
     */
    public static void assertConsistency(BlobStoreRepository repository) {
        final PlainActionFuture<AssertionError> listener = assertConsistencyAsync(repository);
        final AssertionError err = listener.actionGet(TimeValue.timeValueMinutes(1L));
        if (err != null) {
            throw new AssertionError(err);
        }
    }

    /**
     * Same as {@link #assertConsistency(BlobStoreRepository)} but async so it can be used in tests that don't allow blocking.
     */
    public static PlainActionFuture<AssertionError> assertConsistencyAsync(BlobStoreRepository repository) {
        final PlainActionFuture<AssertionError> future = PlainActionFuture.newFuture();
        repository.threadPool().generic().execute(ActionRunnable.wrap(future, listener -> {
            try {
                final BlobContainer blobContainer = repository.blobContainer();
                final long latestGen;
                try (DataInputStream inputStream = new DataInputStream(blobContainer.readBlob("index.latest"))) {
                    latestGen = inputStream.readLong();
                } catch (NoSuchFileException e) {
                    throw new AssertionError("Could not find index.latest blob for repo [" + repository + "]");
                }
                assertIndexGenerations(blobContainer, latestGen);
                final RepositoryData repositoryData;
                try (
                    InputStream blob = blobContainer.readBlob(BlobStoreRepository.INDEX_FILE_PREFIX + latestGen);
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE), blob)
                ) {
                    repositoryData = RepositoryData.snapshotsFromXContent(parser, latestGen, false);
                }
                assertIndexUUIDs(repository, repositoryData);
                assertSnapshotUUIDs(repository, repositoryData, new ActionListener<>() {
                    @Override
                    public void onResponse(AssertionError assertionError) {
                        if (assertionError == null) {
                            try {
                                try {
                                    assertShardIndexGenerations(blobContainer, repositoryData.shardGenerations());
                                } catch (AssertionError e) {
                                    listener.onResponse(e);
                                    return;
                                }
                            } catch (Exception e) {
                                onFailure(e);
                                return;
                            }
                            listener.onResponse(null);
                        } else {
                            listener.onResponse(assertionError);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onResponse(new AssertionError(e));
                    }
                });
            } catch (AssertionError e) {
                listener.onResponse(e);
            }
        }));
        return future;
    }

    private static void assertIndexGenerations(BlobContainer repoRoot, long latestGen) throws IOException {
        final long[] indexGenerations = repoRoot.listBlobsByPrefix(BlobStoreRepository.INDEX_FILE_PREFIX)
            .keySet()
            .stream()
            .map(s -> s.replace(BlobStoreRepository.INDEX_FILE_PREFIX, ""))
            .mapToLong(Long::parseLong)
            .sorted()
            .toArray();
        assertEquals(latestGen, indexGenerations[indexGenerations.length - 1]);
        assertTrue(indexGenerations.length <= 2);
    }

    private static void assertShardIndexGenerations(BlobContainer repoRoot, ShardGenerations shardGenerations) throws IOException {
        final BlobContainer indicesContainer = repoRoot.children().get("indices");
        for (IndexId index : shardGenerations.indices()) {
            final List<ShardGeneration> gens = shardGenerations.getGens(index);
            if (gens.isEmpty() == false) {
                final BlobContainer indexContainer = indicesContainer.children().get(index.getId());
                final Map<String, BlobContainer> shardContainers = indexContainer.children();
                for (int i = 0; i < gens.size(); i++) {
                    final ShardGeneration generation = gens.get(i);
                    assertThat(generation, not(ShardGenerations.DELETED_SHARD_GEN));
                    if (generation != null && generation.equals(ShardGenerations.NEW_SHARD_GEN) == false) {
                        final String shardId = Integer.toString(i);
                        assertThat(shardContainers, hasKey(shardId));
                        assertThat(
                            shardContainers.get(shardId).listBlobsByPrefix(BlobStoreRepository.INDEX_FILE_PREFIX),
                            hasKey(BlobStoreRepository.INDEX_FILE_PREFIX + generation)
                        );
                    }
                }
            }
        }
    }

    private static void assertIndexUUIDs(BlobStoreRepository repository, RepositoryData repositoryData) throws IOException {
        final List<String> expectedIndexUUIDs = repositoryData.getIndices().values().stream().map(IndexId::getId).toList();
        final BlobContainer indicesContainer = repository.blobContainer().children().get("indices");
        final List<String> foundIndexUUIDs;
        if (indicesContainer == null) {
            foundIndexUUIDs = Collections.emptyList();
        } else {
            // Skip Lucene MockFS extraN directory
            foundIndexUUIDs = indicesContainer.children()
                .keySet()
                .stream()
                .filter(s -> s.startsWith("extra") == false)
                .collect(Collectors.toList());
        }
        assertThat(foundIndexUUIDs, containsInAnyOrder(expectedIndexUUIDs.toArray(Strings.EMPTY_ARRAY)));
        for (String indexId : foundIndexUUIDs) {
            final Set<String> indexMetaGenerationsFound = indicesContainer.children()
                .get(indexId)
                .listBlobsByPrefix(BlobStoreRepository.METADATA_PREFIX)
                .keySet()
                .stream()
                .map(p -> p.replace(BlobStoreRepository.METADATA_PREFIX, "").replace(".dat", ""))
                .collect(Collectors.toSet());
            final Set<String> indexMetaGenerationsExpected = new HashSet<>();
            final IndexId idx = repositoryData.getIndices().values().stream().filter(i -> i.getId().equals(indexId)).findFirst().get();
            for (SnapshotId snapshotId : repositoryData.getSnapshots(idx)) {
                indexMetaGenerationsExpected.add(repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, idx));
            }
            // TODO: assertEquals(indexMetaGenerationsExpected, indexMetaGenerationsFound); requires cleanup functionality for
            // index meta generations blobs
            assertTrue(indexMetaGenerationsFound.containsAll(indexMetaGenerationsExpected));
        }
    }

    private static void assertSnapshotUUIDs(
        BlobStoreRepository repository,
        RepositoryData repositoryData,
        ActionListener<AssertionError> listener
    ) throws IOException {
        final BlobContainer repoRoot = repository.blobContainer();
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        final List<String> expectedSnapshotUUIDs = snapshotIds.stream().map(SnapshotId::getUUID).toList();
        for (String prefix : new String[] { BlobStoreRepository.SNAPSHOT_PREFIX, BlobStoreRepository.METADATA_PREFIX }) {
            final Collection<String> foundSnapshotUUIDs = repoRoot.listBlobs()
                .keySet()
                .stream()
                .filter(p -> p.startsWith(prefix))
                .map(p -> p.replace(prefix, "").replace(".dat", ""))
                .collect(Collectors.toSet());
            assertThat(foundSnapshotUUIDs, containsInAnyOrder(expectedSnapshotUUIDs.toArray(Strings.EMPTY_ARRAY)));
        }

        final BlobContainer indicesContainer = repository.getBlobContainer().children().get("indices");
        final Map<String, BlobContainer> indices;
        if (indicesContainer == null) {
            indices = Collections.emptyMap();
        } else {
            indices = indicesContainer.children();
        }
        if (snapshotIds.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        // Assert that for each snapshot, the relevant metadata was written to index and shard folders
        final List<SnapshotInfo> snapshotInfos = Collections.synchronizedList(new ArrayList<>());
        repository.getSnapshotInfo(
            new GetSnapshotInfoContext(
                List.copyOf(snapshotIds),
                true,
                () -> false,
                (ctx, sni) -> snapshotInfos.add(sni),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        try {
                            assertSnapshotInfosConsistency(repository, repositoryData, indices, snapshotInfos);
                        } catch (Exception e) {
                            listener.onResponse(new AssertionError(e));
                            return;
                        } catch (AssertionError e) {
                            listener.onResponse(e);
                            return;
                        }
                        listener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onResponse(new AssertionError(e));
                    }
                }
            )
        );
    }

    private static void assertSnapshotInfosConsistency(
        BlobStoreRepository repository,
        RepositoryData repositoryData,
        Map<String, BlobContainer> indices,
        List<SnapshotInfo> snapshotInfos
    ) throws IOException {
        final Map<IndexId, Integer> maxShardCountsExpected = new HashMap<>();
        final Map<IndexId, Integer> maxShardCountsSeen = new HashMap<>();
        for (SnapshotInfo snapshotInfo : snapshotInfos) {
            final SnapshotId snapshotId = snapshotInfo.snapshotId();
            for (String index : snapshotInfo.indices()) {
                final IndexId indexId = repositoryData.resolveIndexId(index);
                assertThat(indices, hasKey(indexId.getId()));
                final BlobContainer indexContainer = indices.get(indexId.getId());
                assertThat(
                    indexContainer.listBlobs(),
                    hasKey(
                        String.format(
                            Locale.ROOT,
                            BlobStoreRepository.METADATA_NAME_FORMAT,
                            repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId)
                        )
                    )
                );
                final IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
                for (Map.Entry<String, BlobContainer> entry : indexContainer.children().entrySet()) {
                    // Skip Lucene MockFS extraN directory
                    if (entry.getKey().startsWith("extra")) {
                        continue;
                    }
                    final int shardId = Integer.parseInt(entry.getKey());
                    final int shardCount = indexMetadata.getNumberOfShards();
                    maxShardCountsExpected.compute(
                        indexId,
                        (i, existing) -> existing == null || existing < shardCount ? shardCount : existing
                    );
                    final BlobContainer shardContainer = entry.getValue();
                    // TODO: we shouldn't be leaking empty shard directories when a shard (but not all of the index it belongs to)
                    // becomes unreferenced. We should fix that and remove this conditional once its fixed.
                    if (shardContainer.listBlobs().keySet().stream().anyMatch(blob -> blob.startsWith("extra") == false)) {
                        final int impliedCount = shardId - 1;
                        maxShardCountsSeen.compute(
                            indexId,
                            (i, existing) -> existing == null || existing < impliedCount ? impliedCount : existing
                        );
                    }
                    if (shardId < shardCount
                        && snapshotInfo.shardFailures()
                            .stream()
                            .noneMatch(shardFailure -> shardFailure.index().equals(index) && shardFailure.shardId() == shardId)) {
                        final Map<String, BlobMetadata> shardPathContents = shardContainer.listBlobs();
                        assertThat(
                            shardPathContents,
                            hasKey(String.format(Locale.ROOT, BlobStoreRepository.SNAPSHOT_NAME_FORMAT, snapshotId.getUUID()))
                        );
                        assertThat(
                            shardPathContents.keySet()
                                .stream()
                                .filter(name -> name.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))
                                .count(),
                            lessThanOrEqualTo(2L)
                        );
                        final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots = repository.getBlobStoreIndexShardSnapshots(
                            indexId,
                            shardId,
                            repositoryData.shardGenerations().getShardGen(indexId, shardId)
                        );
                        assertTrue(
                            blobStoreIndexShardSnapshots.snapshots()
                                .stream()
                                .anyMatch(snapshotFiles -> snapshotFiles.snapshot().equals(snapshotId.getName()))
                        );
                    }
                }
            }
        }
        maxShardCountsSeen.forEach(
            ((indexId, count) -> assertThat(
                "Found unreferenced shard paths for index [" + indexId + "]",
                count,
                lessThanOrEqualTo(maxShardCountsExpected.get(indexId))
            ))
        );
    }

    public static void assertBlobsByPrefix(BlobStoreRepository repository, BlobPath path, String prefix, Map<String, BlobMetadata> blobs) {
        final PlainActionFuture<Map<String, BlobMetadata>> future = PlainActionFuture.newFuture();
        repository.threadPool()
            .generic()
            .execute(ActionRunnable.supply(future, () -> repository.blobStore().blobContainer(path).listBlobsByPrefix(prefix)));
        Map<String, BlobMetadata> foundBlobs = future.actionGet();
        if (blobs.isEmpty()) {
            assertThat(foundBlobs.keySet(), empty());
        } else {
            assertThat(foundBlobs.keySet(), containsInAnyOrder(blobs.keySet().toArray(Strings.EMPTY_ARRAY)));
            for (Map.Entry<String, BlobMetadata> entry : foundBlobs.entrySet()) {
                assertEquals(entry.getValue().length(), blobs.get(entry.getKey()).length());
            }
        }
    }

    /**
     * Creates a mocked {@link ClusterService} for use in {@link BlobStoreRepository} related tests that mocks out all the necessary
     * functionality to make {@link BlobStoreRepository} work. Initializes the cluster state as {@link ClusterState#EMPTY_STATE}.
     *
     * @return Mock ClusterService
     */
    public static ClusterService mockClusterService() {
        return mockClusterService(ClusterState.EMPTY_STATE);
    }

    /**
     * Creates a mocked {@link ClusterService} for use in {@link BlobStoreRepository} related tests that mocks out all the necessary
     * functionality to make {@link BlobStoreRepository} work. Initializes the cluster state with a {@link RepositoriesMetadata} instance
     * that contains the given {@code metadata}.
     *
     * @param metadata RepositoryMetadata to initialize the cluster state with
     * @return Mock ClusterService
     */
    public static ClusterService mockClusterService(RepositoryMetadata metadata) {
        return mockClusterService(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .metadata(
                    Metadata.builder()
                        .clusterUUID(UUIDs.randomBase64UUID(random()))
                        .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(Collections.singletonList(metadata)))
                        .build()
                )
                .build()
        );
    }

    private static ClusterService mockClusterService(ClusterState initialState) {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(threadPool.executor(ThreadPool.Names.SNAPSHOT)).thenReturn(new SameThreadExecutorService());
        when(threadPool.executor(ThreadPool.Names.SNAPSHOT_META)).thenReturn(new SameThreadExecutorService());
        when(threadPool.generic()).thenReturn(new SameThreadExecutorService());
        when(threadPool.info(ThreadPool.Names.SNAPSHOT)).thenReturn(
            new ThreadPool.Info(ThreadPool.Names.SNAPSHOT, ThreadPool.ThreadPoolType.FIXED, randomIntBetween(1, 10))
        );
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        // Setting local node as master so it may update the repository metadata in the cluster state
        final DiscoveryNode localNode = DiscoveryNodeUtils.create("");
        when(clusterService.localNode()).thenReturn(localNode);
        final AtomicReference<ClusterState> currentState = new AtomicReference<>(
            ClusterState.builder(initialState)
                .nodes(DiscoveryNodes.builder().add(localNode).masterNodeId(localNode.getId()).localNodeId(localNode.getId()).build())
                .build()
        );
        when(clusterService.state()).then(invocationOnMock -> currentState.get());
        final List<ClusterStateApplier> appliers = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            final ClusterStateUpdateTask task = ((ClusterStateUpdateTask) invocation.getArguments()[1]);
            final ClusterState current = currentState.get();
            final ClusterState next = task.execute(current);
            currentState.set(next);
            appliers.forEach(
                applier -> applier.applyClusterState(new ClusterChangedEvent((String) invocation.getArguments()[0], next, current))
            );
            task.clusterStateProcessed(current, next);
            return null;
        }).when(clusterService).submitUnbatchedStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        doAnswer(invocation -> {
            appliers.add((ClusterStateApplier) invocation.getArguments()[0]);
            return null;
        }).when(clusterService).addStateApplier(any(ClusterStateApplier.class));
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        return clusterService;
    }
}
