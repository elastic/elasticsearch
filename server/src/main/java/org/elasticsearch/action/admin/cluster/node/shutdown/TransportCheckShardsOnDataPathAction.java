/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class TransportCheckShardsOnDataPathAction extends TransportNodesAction<
    CheckShardsOnDataPathRequest,
    CheckShardsOnDataPathResponse,
    NodeCheckShardsOnDataPathRequest,
    NodeCheckShardsOnDataPathResponse> {

    public static final String ACTION_NAME = "internal:admin/indices/check_shards_on_data_path";
    public static final ActionType<CheckShardsOnDataPathResponse> TYPE = new ActionType<>(ACTION_NAME, CheckShardsOnDataPathResponse::new);
    private static final Logger logger = LogManager.getLogger(TransportCheckShardsOnDataPathAction.class);

    private final TransportService transportService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportCheckShardsOnDataPathAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeEnvironment nodeEnv
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            CheckShardsOnDataPathRequest::new,
            NodeCheckShardsOnDataPathRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeCheckShardsOnDataPathResponse.class
        );
        this.transportService = transportService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    protected CheckShardsOnDataPathResponse newResponse(
        CheckShardsOnDataPathRequest request,
        List<NodeCheckShardsOnDataPathResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new CheckShardsOnDataPathResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeCheckShardsOnDataPathRequest newNodeRequest(CheckShardsOnDataPathRequest request) {
        return new NodeCheckShardsOnDataPathRequest(request.getShardIds());
    }

    @Override
    protected NodeCheckShardsOnDataPathResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeCheckShardsOnDataPathResponse(in);
    }

    @Override
    protected NodeCheckShardsOnDataPathResponse nodeOperation(NodeCheckShardsOnDataPathRequest request, Task task) {
        Set<ShardId> localShards = new HashSet<>();
        ShardPath shardPath = null;
        for (ShardId shardId : request.getShardIDs()) {
            try {
                shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, null);
                if (shardPath != null) {
                    Store.tryOpenIndex(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
                }
                localShards.add(shardId);
            } catch (IOException e) {
                final String path = shardPath != null ? shardPath.resolveIndex().toString() : "";
                logger.debug(() -> String.format(Locale.ROOT, "cannot open index for shard [%s] in path [%s]", shardId, path), e);
            }
        }
        return new NodeCheckShardsOnDataPathResponse(transportService.getLocalNode(), localShards);
    }
}
