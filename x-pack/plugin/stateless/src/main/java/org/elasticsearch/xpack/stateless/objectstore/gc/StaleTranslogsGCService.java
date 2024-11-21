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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StaleTranslogsGCService {
    private final Logger logger = LogManager.getLogger(StaleTranslogsGCService.class);

    private final Supplier<ObjectStoreService> objectStoreService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ThreadContext threadContext;
    private final Client client;
    private final int filesLimit;

    public StaleTranslogsGCService(
        Supplier<ObjectStoreService> objectStoreService,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Settings settings
    ) {
        this.objectStoreService = objectStoreService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.threadContext = threadPool.getThreadContext();
        this.client = client;
        this.filesLimit = ObjectStoreGCTask.STALE_TRANSLOGS_GC_FILES_LIMIT_SETTING.get(settings);
    }

    void cleanStaleTranslogs(ActionListener<Void> listener, ActionListener<Void> listenerOnConsistentClusterState) {
        try {
            logger.trace("Translog GC runs");
            Set<String> translogEphemeralIds = objectStoreService.get().getNodesWithTranslogBlobContainers();

            Set<String> clusterEphemeralIds = clusterService.state()
                .nodes()
                .stream()
                .map(dn -> dn.getEphemeralId())
                .collect(Collectors.toUnmodifiableSet());
            Set<String> staleEphemeralIds = Sets.difference(translogEphemeralIds, clusterEphemeralIds);

            if (staleEphemeralIds.isEmpty() == false) {
                Map<String, Set<String>> staleEphemeralIdsFiles = new HashMap<>();
                int filesFound = 0;
                for (String staleEphemeralId : staleEphemeralIds) {
                    Set<String> staleEphemeralFiles = new HashSet<>();
                    objectStoreService.get()
                        .getTranslogBlobContainer(staleEphemeralId)
                        .listBlobs(OperationPurpose.TRANSLOG)
                        .keySet()
                        .forEach(s -> staleEphemeralFiles.add(s)); // keys are copied so that associated BlobMetadata are not kept alive
                    staleEphemeralIdsFiles.put(staleEphemeralId, staleEphemeralFiles);
                    filesFound += staleEphemeralFiles.size();
                    if (filesFound >= filesLimit) {
                        logger.trace(
                            "Translog GC stops iterating stale nodes after finding [{}] files due to the [{}={}] limit",
                            filesFound,
                            ObjectStoreGCTask.STALE_TRANSLOGS_GC_FILES_LIMIT_SETTING.getKey(),
                            filesLimit
                        );
                        break;
                    }
                }
                logger.trace("Translog GC found stale translog files [{}]", staleEphemeralIdsFiles);

                SubscribableListener.newForked(this::doConsistentClusterStateRead)
                    .<Void>andThen(threadPool.generic(), threadContext, (l, state) -> {
                        listenerOnConsistentClusterState.onResponse(null);
                        deleteStaleTranslogContainers(listener, state, staleEphemeralIdsFiles);
                    })
                    .addListener(listener);
            } else {
                listener.onResponse(null);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void doConsistentClusterStateRead(ActionListener<ClusterState> listener) {
        client.execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request(),
            listener.map(TransportConsistentClusterStateReadAction.Response::getState)
        );
    }

    private void deleteStaleTranslogContainers(
        ActionListener<Void> listener,
        ClusterState state,
        Map<String, Set<String>> staleEphemeralIdsFiles
    ) {
        ActionListener.completeWith(listener, () -> {
            final var project = state.metadata().getProject();
            if (new ClusterStateHealth(state, project.getConcreteAllIndices(), project.id()).getStatus() == ClusterHealthStatus.RED) {
                logger.debug("Translog GC did not proceed because cluster state health is red");
                return null;
            }

            Set<String> clusterEphemeralIds = state.nodes().stream().map(dn -> dn.getEphemeralId()).collect(Collectors.toUnmodifiableSet());
            for (String staleEphemeralId : staleEphemeralIdsFiles.keySet()) {
                if (clusterEphemeralIds.contains(staleEphemeralId)) {
                    continue;
                }
                Set<String> staleFiles = staleEphemeralIdsFiles.get(staleEphemeralId);
                try {
                    logger.debug("Translog GC deleting stale node ephemeral ID [{}] files {}", staleEphemeralId, staleFiles);
                    objectStoreService.get()
                        .getTranslogBlobContainer(staleEphemeralId)
                        .deleteBlobsIgnoringIfNotExists(OperationPurpose.TRANSLOG, staleFiles.iterator());
                } catch (IOException e) {
                    logger.debug(
                        "Unable to delete stale node ephemeral ID container ["
                            + staleEphemeralId
                            + "] files ["
                            + staleFiles
                            + "] from the object store. It will be deleted eventually",
                        e
                    );
                }
            }
            return null;
        });
    }
}
