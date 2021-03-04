/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.delete;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.elasticsearch.action.admin.indices.dangling.list.NodeListDanglingIndicesResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the deletion of a dangling index. When handling a {@link DeleteDanglingIndexAction},
 * this class first checks that such a dangling index exists. It then submits a cluster state update
 * to add the index to the index graveyard.
 */
public class TransportDeleteDanglingIndexAction extends AcknowledgedTransportMasterNodeAction<DeleteDanglingIndexRequest> {
    private static final Logger logger = LogManager.getLogger(TransportDeleteDanglingIndexAction.class);

    private final Settings settings;
    private final NodeClient nodeClient;

    @Inject
    public TransportDeleteDanglingIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        NodeClient nodeClient
    ) {
        super(
            DeleteDanglingIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDanglingIndexRequest::new,
            indexNameExpressionResolver,
            ThreadPool.Names.GENERIC
        );
        this.settings = settings;
        this.nodeClient = nodeClient;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDanglingIndexRequest deleteRequest,
        ClusterState state,
        ActionListener<AcknowledgedResponse> deleteListener
    ) throws Exception {
        findDanglingIndex(deleteRequest.getIndexUUID(), new ActionListener<>() {
            @Override
            public void onResponse(Index indexToDelete) {
                // This flag is checked at this point so that we always check that the supplied index ID
                // does correspond to a dangling index.
                if (deleteRequest.isAcceptDataLoss() == false) {
                    deleteListener.onFailure(new IllegalArgumentException("accept_data_loss must be set to true"));
                    return;
                }

                String indexName = indexToDelete.getName();
                String indexUUID = indexToDelete.getUUID();

                final ActionListener<AcknowledgedResponse> clusterStateUpdatedListener = deleteListener.delegateResponse((l, e) -> {
                    logger.debug("Failed to delete dangling index [" + indexName + "] [" + indexUUID + "]", e);
                    l.onFailure(e);
                });

                final String taskSource = "delete-dangling-index [" + indexName + "] [" + indexUUID + "]";

                clusterService.submitStateUpdateTask(
                    taskSource, new AckedClusterStateUpdateTask(deleteRequest, clusterStateUpdatedListener) {
                        @Override
                        public ClusterState execute(final ClusterState currentState) {
                            return deleteDanglingIndex(currentState, indexToDelete);
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Failed to find dangling index [" + deleteRequest.getIndexUUID() + "]", e);
                deleteListener.onFailure(e);
            }
        });
    }

    private ClusterState deleteDanglingIndex(ClusterState currentState, Index indexToDelete) {
        final Metadata metaData = currentState.getMetadata();

        for (ObjectObjectCursor<String, IndexMetadata> each : metaData.indices()) {
            if (indexToDelete.getUUID().equals(each.value.getIndexUUID())) {
                throw new IllegalArgumentException(
                    "Refusing to delete dangling index "
                        + indexToDelete
                        + " as an index with UUID ["
                        + indexToDelete.getUUID()
                        + "] already exists in the cluster state"
                );
            }
        }

        // By definition, a dangling index is an index not present in the cluster state and with no tombstone,
        // so we shouldn't reach this point if these conditions aren't met. For super-safety, however, check
        // that a tombstone doesn't already exist for this index.
        if (metaData.indexGraveyard().containsIndex(indexToDelete)) {
            return currentState;
        }

        Metadata.Builder metaDataBuilder = Metadata.builder(metaData);

        final IndexGraveyard newGraveyard = IndexGraveyard.builder(metaDataBuilder.indexGraveyard())
            .addTombstone(indexToDelete)
            .build(settings);
        metaDataBuilder.indexGraveyard(newGraveyard);

        return ClusterState.builder(currentState).metadata(metaDataBuilder.build()).build();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDanglingIndexRequest request, ClusterState state) {
        return null;
    }

    private void findDanglingIndex(String indexUUID, ActionListener<Index> listener) {
        this.nodeClient.execute(ListDanglingIndicesAction.INSTANCE, new ListDanglingIndicesRequest(indexUUID), listener.delegateFailure(
            (l, response) -> {
                if (response.hasFailures()) {
                    final String nodeIds = response.failures().stream().map(FailedNodeException::nodeId).collect(Collectors.joining(","));
                    ElasticsearchException e = new ElasticsearchException("Failed to query nodes [" + nodeIds + "]");

                    for (FailedNodeException failure : response.failures()) {
                        logger.error("Failed to query node [" + failure.nodeId() + "]", failure);
                        e.addSuppressed(failure);
                    }

                    l.onFailure(e);
                    return;
                }

                final List<NodeListDanglingIndicesResponse> nodes = response.getNodes();

                for (NodeListDanglingIndicesResponse nodeResponse : nodes) {
                    for (DanglingIndexInfo each : nodeResponse.getDanglingIndices()) {
                        if (each.getIndexUUID().equals(indexUUID)) {
                            l.onResponse(new Index(each.getIndexName(), each.getIndexUUID()));
                            return;
                        }
                    }
                }
                l.onFailure(new IllegalArgumentException("No dangling index found for UUID [" + indexUUID + "]"));
        }));
    }
}
