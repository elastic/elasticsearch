/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.unpromotable.TransportBroadcastUnpromotableAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportUnpromotableShardRefreshAction extends TransportBroadcastUnpromotableAction<
    UnpromotableShardRefreshRequest,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportUnpromotableShardRefreshAction.class);

    public static final String NAME = "indices:admin/refresh/unpromotable";

    static {
        // noinspection ConstantValue just for documentation
        assert NAME.equals(RefreshAction.NAME + "/unpromotable");
    }

    private final IndicesService indicesService;

    @Inject
    public TransportUnpromotableShardRefreshAction(
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            shardStateAction,
            actionFilters,
            UnpromotableShardRefreshRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.REFRESH)
        );
        this.indicesService = indicesService;
    }

    @Override
    protected void unpromotableShardOperation(
        Task task,
        UnpromotableShardRefreshRequest request,
        ActionListener<ActionResponse.Empty> responseListener
    ) {
        if (clusterService.state().version() >= request.getClusterStateVersion()) {
            doUnpromotableShardOperation(request, responseListener);
        } else {
            final Thread initialThread = Thread.currentThread();
            ClusterStateObserver.waitForState(
                clusterService,
                clusterService.threadPool().getThreadContext(),
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        if (initialThread == Thread.currentThread()) {
                            doUnpromotableShardOperation(request, responseListener);
                        } else {
                            executor.execute(ActionRunnable.wrap(responseListener, l -> doUnpromotableShardOperation(request, l)));
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        responseListener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        assert false : "Timed out waiting for cluster state";
                        responseListener.onFailure(new ElasticsearchException("Timed out waiting for cluster state"));
                    }
                },
                state -> state.version() >= request.getClusterStateVersion(),
                TimeValue.MAX_VALUE,
                logger
            );
        }
    }

    // package private for testing
    void doUnpromotableShardOperation(
        UnpromotableShardRefreshRequest request,
        ActionListener<ActionResponse.Empty> responseListener
    ) {
        ActionListener.run(responseListener, listener -> {
            IndexShard shard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
            shard.waitForPrimaryTermAndGeneration(
                request.getPrimaryTerm(),
                request.getSegmentGeneration(),
                listener.map(l -> ActionResponse.Empty.INSTANCE)
            );
        });
    }

    @Override
    protected ActionResponse.Empty combineUnpromotableShardResponses(List<ActionResponse.Empty> empties) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty readResponse(StreamInput in) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty emptyResponse() {
        return ActionResponse.Empty.INSTANCE;
    }
}
