/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action.cache;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportSearchableSnapshotCacheStoresAction extends TransportNodesAction<
    TransportNodesListShardStoreMetadata.Request,
    TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata,
    TransportNodesListShardStoreMetadata.NodeRequest,
    TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> {

    public static final String ACTION_NAME = "cluster:admin/xpack/searchable_snapshots/cache/store";

    public static final ActionType<TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata> TYPE = new ActionType<>(
        ACTION_NAME,
        TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata::new
    );

    protected TransportSearchableSnapshotCacheStoresAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<TransportNodesListShardStoreMetadata.Request> request,
        Writeable.Reader<TransportNodesListShardStoreMetadata.NodeRequest> nodeRequest,
        String nodeExecutor,
        String finalExecutor,
        Class<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> nodeStoreFilesMetadataClass
    ) {
        super(
            actionName,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            request,
            nodeRequest,
            nodeExecutor,
            finalExecutor,
            nodeStoreFilesMetadataClass
        );
    }

    @Override
    protected TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata newResponse(
        TransportNodesListShardStoreMetadata.Request request,
        List<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> nodeStoreFilesMetadata,
        List<FailedNodeException> failures
    ) {
        throw new AssertionError("TODO");
    }

    @Override
    protected TransportNodesListShardStoreMetadata.NodeRequest newNodeRequest(TransportNodesListShardStoreMetadata.Request request) {
        throw new AssertionError("TODO");
    }

    @Override
    protected TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata newNodeResponse(StreamInput in) throws IOException {
        throw new AssertionError("TODO");
    }

    @Override
    protected TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata nodeOperation(
        TransportNodesListShardStoreMetadata.NodeRequest request,
        Task task
    ) {
        throw new AssertionError("TODO");
    }
}
