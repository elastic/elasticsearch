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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
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

    private Set<String> getStaleIndicesUUIDs() throws IOException {
        var indicesBlobContainer = objectStoreService().getIndicesBlobContainer();
        var indicesUUIDsInBlobStore = indicesBlobContainer.children(OperationPurpose.INDICES).keySet();
        var clusterState = clusterService.state();

        var staleIndexUUIDs = new HashSet<>(indicesUUIDsInBlobStore);
        for (IndexMetadata indexMetadata : clusterState.metadata().getProject()) {
            staleIndexUUIDs.remove(indexMetadata.getIndexUUID());
        }

        return Collections.unmodifiableSet(staleIndexUUIDs);
    }

    private void doConsistentClusterStateRead(ActionListener<ClusterState> listener) {
        client.execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request(),
            listener.map(TransportConsistentClusterStateReadAction.Response::getState)
        );
    }

    private void deleteStaleIndices(ActionListener<Void> listener, ClusterState state, Set<String> localStateStaleIndexUUIDs) {
        ActionListener.completeWith(listener, () -> {
            var staleIndexUUIDs = new HashSet<>(localStateStaleIndexUUIDs);
            // This could happen if the node performing the cleanup is behind the latest cluster state
            // and a new index was created while the node was behind. If that's the case it means that
            // the index is not stale, and it must not be deleted.
            state.metadata().getProject().stream().map(IndexMetadata::getIndexUUID).forEach(localStateStaleIndexUUIDs::remove);

            logger.debug("Delete stale indices [{}] from the object store", localStateStaleIndexUUIDs);
            for (String staleIndexUUID : staleIndexUUIDs) {

                try {
                    logger.debug("Deleting stale index [{}]", staleIndexUUID);
                    objectStoreService().getIndexBlobContainer(staleIndexUUID).delete(OperationPurpose.INDICES);
                } catch (IOException e) {
                    logger.debug(
                        "Unable to delete stale index [" + staleIndexUUID + "] from the object store. It will be deleted eventually",
                        e
                    );
                }
            }
            return null;
        });
    }

    private ObjectStoreService objectStoreService() {
        return objectStoreService.get();
    }
}
