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

package co.elastic.elasticsearch.stateless.objectstore.gc;

import co.elastic.elasticsearch.stateless.cluster.coordination.TransportConsistentClusterStateReadAction;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class StaleIndicesGCService {
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

    private ObjectStoreService objectStoreService() {
        return objectStoreService.get();
    }
}
