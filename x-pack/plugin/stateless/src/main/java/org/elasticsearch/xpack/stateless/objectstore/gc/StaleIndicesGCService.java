/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore.gc;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cluster.coordination.TransportConsistentClusterStateReadAction;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class StaleIndicesGCService {
    // Bound retained candidates per GC cycle; subsequent cycles collect the remaining files.
    private static final int STALE_SHARD_FILES_BATCH_SIZE = 10_000;

    private final Logger logger = LogManager.getLogger(StaleIndicesGCService.class);

    private final Supplier<ObjectStoreService> objectStoreService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ThreadContext threadContext;
    private final Client client;

    public StaleIndicesGCService(
        Supplier<ObjectStoreService> objectStoreService,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client
    ) {
        this.objectStoreService = objectStoreService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.threadContext = threadPool.getThreadContext();
        this.client = client;
    }

    void cleanStaleIndices(ActionListener<Void> listener) {
        try {
            var staleIndexUUIDs = getStaleIndicesUUIDs();

            if (staleIndexUUIDs.isEmpty()) {
                listener.onResponse(null);
                return;
            }

            SubscribableListener.newForked(this::doConsistentClusterStateRead)
                .<Void>andThen(threadPool.generic(), threadContext, (l, state) -> deleteStaleIndices(listener, state, staleIndexUUIDs))
                .addListener(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // Package private for testing
    Map<ProjectId, Set<String>> getStaleIndicesUUIDs() throws IOException {
        var clusterState = clusterService.state();
        Map<ProjectId, Set<String>> staleIndexUUIDs = new HashMap<>();
        for (var project : clusterState.metadata().projects().values()) {
            final var projectId = project.id();

            BlobContainer indicesBlobContainer;
            try {
                indicesBlobContainer = objectStoreService().getIndicesBlobContainer(projectId);
            } catch (RepositoryException e) {
                // Skip if the project is concurrently deleted. They will be picked up again if the project is later resurrected.
                // TODO: See ES-12120 for adding an IT for this case.
                logger.info(
                    "skip getting stale indices for project [{}], cannot get its indices blob container, reason: [{}]",
                    projectId,
                    e.getMessage()
                );
                continue;
            }
            Set<String> indicesUUIDsInBlobStore = indicesBlobContainer.children(OperationPurpose.INDICES).keySet();
            var staleIndexUUIDsOneProject = new HashSet<>(indicesUUIDsInBlobStore);
            for (IndexMetadata indexMetadata : project) {
                staleIndexUUIDsOneProject.remove(indexMetadata.getIndexUUID());
            }
            if (staleIndexUUIDsOneProject.isEmpty() == false) {
                staleIndexUUIDs.put(projectId, Collections.unmodifiableSet(staleIndexUUIDsOneProject));
            }
        }
        return Collections.unmodifiableMap(staleIndexUUIDs);
    }

    private void doConsistentClusterStateRead(ActionListener<ClusterState> listener) {
        client.execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request(),
            listener.map(TransportConsistentClusterStateReadAction.Response::getState)
        );
    }

    // Package private for testing
    void deleteStaleIndices(ActionListener<Void> listener, ClusterState state, Map<ProjectId, Set<String>> localStateStaleIndexUUIDs) {
        ActionListener.completeWith(listener, () -> {
            for (var projectId : localStateStaleIndexUUIDs.keySet()) {
                if (state.metadata().hasProject(projectId) == false) {
                    logger.debug("project [{}] not found, skipping stale indices cleanup", projectId);
                    continue;
                }

                var staleIndexUUIDs = new HashSet<>(localStateStaleIndexUUIDs.get(projectId));
                // This could happen if the node performing the cleanup is behind the latest cluster state
                // and a new index was created while the node was behind. If that's the case it means that
                // the index is not stale, and it must not be deleted.
                state.metadata().getProject(projectId).stream().map(IndexMetadata::getIndexUUID).forEach(staleIndexUUIDs::remove);

                logger.debug("Delete stale indices [{}] from the object store", staleIndexUUIDs);
                for (String staleIndexUUID : staleIndexUUIDs) {

                    final BlobContainer blobContainer;
                    try {
                        blobContainer = objectStoreService().getIndexBlobContainer(projectId, staleIndexUUID);
                    } catch (RepositoryException e) {
                        // Skip deletion if the project is concurrently deleted. They will be deleted if the project is later resurrected.
                        // TODO: See ES-12120 for adding an IT for this case.
                        logger.info(
                            "skip deleting stale indices for project [{}], cannot get its index blob container, reason: [{}]",
                            projectId,
                            e.getMessage()
                        );
                        continue;
                    }
                    try {
                        logger.debug("Deleting stale index [{}]", staleIndexUUID);
                        blobContainer.delete(OperationPurpose.INDICES);
                    } catch (RepositoryException | AlreadyClosedException | IOException e) {
                        logger.debug(
                            "Unable to delete stale index [" + staleIndexUUID + "] from the object store. It will be deleted eventually",
                            e
                        );
                    }
                }
            }
            return null;
        });
    }

    /** Cleans shard IDs removed by a restore without deleting the surviving index's blob container. */
    void cleanStaleShardFiles(ActionListener<Void> listener) {
        try {
            final var candidates = getStaleShardFiles();
            if (candidates.isEmpty()) {
                listener.onResponse(null);
                return;
            }
            SubscribableListener.newForked(this::doConsistentClusterStateRead)
                .<Void>andThen(threadPool.generic(), threadContext, (l, state) -> deleteStaleShardFiles(l, state, candidates))
                .addListener(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // Capture individual blob names before the consistent read. Never recursively delete a removed shard's container: a later
    // restore/reshard can reuse the ID while deletion is in flight. Restore carries the old maximum primary term forward so that
    // newly written blobs use a different primary-term namespace.
    record StaleShardFiles(ProjectId projectId, Index index, int shardId, String history, BlobContainer container, Set<String> names) {}

    List<StaleShardFiles> getStaleShardFiles() throws IOException {
        return getStaleShardFiles(STALE_SHARD_FILES_BATCH_SIZE);
    }

    // Package-private to exercise multiple batches without creating thousands of blobs in tests.
    List<StaleShardFiles> getStaleShardFiles(int batchSize) throws IOException {
        assert batchSize > 0;
        int remaining = batchSize;
        final List<StaleShardFiles> candidates = new ArrayList<>();
        for (var project : clusterService.state().metadata().projects().values()) {
            for (IndexMetadata index : project) {
                final String history = index.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID);
                if (history == null) {
                    continue;
                }
                final BlobContainer indexContainer;
                try {
                    indexContainer = objectStoreService().getIndexBlobContainer(project.id(), index.getIndexUUID());
                } catch (RepositoryException e) {
                    continue; // project removal is concurrent with GC
                }
                for (var shard : indexContainer.children(OperationPurpose.INDICES).entrySet()) {
                    final int shardId = Integer.parseInt(shard.getKey());
                    if (shardId >= index.getNumberOfShards()) {
                        remaining = collectStaleShardFiles(
                            project.id(),
                            index.getIndex(),
                            shardId,
                            history,
                            shard.getValue(),
                            candidates,
                            remaining
                        );
                        if (remaining == 0) {
                            return List.copyOf(candidates);
                        }
                    }
                }
            }
        }
        return List.copyOf(candidates);
    }

    private static int collectStaleShardFiles(
        ProjectId projectId,
        Index index,
        int shardId,
        String history,
        BlobContainer container,
        List<StaleShardFiles> candidates,
        int remaining
    ) throws IOException {
        // BlobContainer has no paginated listing API, so one container's listing is still materialized temporarily.
        // Retain only the names that fit in this cycle's batch, rather than retaining listings across the whole cluster.
        final Set<String> names = new HashSet<>();
        for (String name : container.listBlobs(OperationPurpose.INDICES).keySet()) {
            names.add(name);
            if (--remaining == 0) {
                break;
            }
        }
        if (names.isEmpty() == false) {
            candidates.add(new StaleShardFiles(projectId, index, shardId, history, container, Set.copyOf(names)));
        }
        if (remaining == 0) {
            return 0;
        }
        for (BlobContainer child : container.children(OperationPurpose.INDICES).values()) {
            remaining = collectStaleShardFiles(projectId, index, shardId, history, child, candidates, remaining);
            if (remaining == 0) {
                return 0;
            }
        }
        return remaining;
    }

    void deleteStaleShardFiles(ActionListener<Void> listener, ClusterState state, List<StaleShardFiles> candidates) {
        ActionListener.completeWith(listener, () -> {
            for (StaleShardFiles candidate : candidates) {
                if (state.metadata().hasProject(candidate.projectId()) == false) {
                    continue;
                }
                final var index = state.metadata().getProject(candidate.projectId()).index(candidate.index());
                if (index == null
                    || candidate.shardId() < index.getNumberOfShards()
                    || candidate.history().equals(index.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID)) == false) {
                    continue;
                }
                candidate.container().deleteBlobsIgnoringIfNotExists(OperationPurpose.INDICES, candidate.names().iterator());
            }
            return null;
        });
    }

    private ObjectStoreService objectStoreService() {
        return objectStoreService.get();
    }
}
