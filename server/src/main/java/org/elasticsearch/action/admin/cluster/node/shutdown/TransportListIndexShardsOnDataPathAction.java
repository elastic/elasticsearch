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

public class TransportListIndexShardsOnDataPathAction extends TransportNodesAction<
    ListIndexShardsOnDataPathRequest,
    ListIndexShardsOnDataPathResponse,
    NodeListIndexShardsOnDataPathRequest,
    NodeListIndexShardsOnDataPathResponse> {

    public static final String ACTION_NAME = "internal:admin/indices/shards_on_data_path/list";
    public static final ActionType<ListIndexShardsOnDataPathResponse> TYPE = new ActionType<>(
        ACTION_NAME,
        ListIndexShardsOnDataPathResponse::new
    );
    private static final Logger logger = LogManager.getLogger(TransportListIndexShardsOnDataPathAction.class);

    private final TransportService transportService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportListIndexShardsOnDataPathAction(
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
            ListIndexShardsOnDataPathRequest::new,
            NodeListIndexShardsOnDataPathRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeListIndexShardsOnDataPathResponse.class
        );
        this.transportService = transportService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    protected ListIndexShardsOnDataPathResponse newResponse(
        ListIndexShardsOnDataPathRequest request,
        List<NodeListIndexShardsOnDataPathResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new ListIndexShardsOnDataPathResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeListIndexShardsOnDataPathRequest newNodeRequest(ListIndexShardsOnDataPathRequest request) {
        return new NodeListIndexShardsOnDataPathRequest(request.getShardIds());
    }

    @Override
    protected NodeListIndexShardsOnDataPathResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeListIndexShardsOnDataPathResponse(in);
    }

    @Override
    protected NodeListIndexShardsOnDataPathResponse nodeOperation(NodeListIndexShardsOnDataPathRequest request, Task task) {
        Set<ShardId> localShards = new HashSet<>();
        ShardPath shardPath = null;
        String customDataPath = request.getCustomDataPath();
        for (ShardId shardId : request.getShardIDs()) {
            try {
                shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
                if (shardPath != null) {
                    Store.tryOpenIndex(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
                }
                localShards.add(shardId);
            } catch (IOException e) {
                final String path = shardPath != null ? shardPath.resolveIndex().toString() : "";
                logger.debug(() -> String.format(Locale.ROOT, "cannot open index for shard [%s] in path [%s]", shardId, path), e);
            }
        }
        return new NodeListIndexShardsOnDataPathResponse(transportService.getLocalNode(), localShards);
    }
}
