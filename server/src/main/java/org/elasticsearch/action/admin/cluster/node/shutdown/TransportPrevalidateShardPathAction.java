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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Given a set of shard IDs, checks which of those shards have a matching directory in the local data path.
 * This is used by {@link PrevalidateNodeRemovalAction} to find out whether a node may contain some copy
 * of a specific shard. The response contains a subset of the request shard IDs which are in the cluster state
 * of this node and have a matching shard path on the local data path.
 */
public class TransportPrevalidateShardPathAction extends TransportNodesAction<
    PrevalidateShardPathRequest,
    PrevalidateShardPathResponse,
    NodePrevalidateShardPathRequest,
    NodePrevalidateShardPathResponse> {

    public static final String ACTION_NAME = "internal:admin/indices/prevalidate_shard_path";
    public static final ActionType<PrevalidateShardPathResponse> TYPE = new ActionType<>(ACTION_NAME);
    private static final Logger logger = LogManager.getLogger(TransportPrevalidateShardPathAction.class);

    private final TransportService transportService;
    private final NodeEnvironment nodeEnv;
    private final Settings settings;

    @Inject
    public TransportPrevalidateShardPathAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeEnvironment nodeEnv,
        Settings settings
    ) {
        super(
            ACTION_NAME,
            clusterService,
            transportService,
            actionFilters,
            NodePrevalidateShardPathRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.transportService = transportService;
        this.nodeEnv = nodeEnv;
        this.settings = settings;
    }

    @Override
    protected PrevalidateShardPathResponse newResponse(
        PrevalidateShardPathRequest request,
        List<NodePrevalidateShardPathResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new PrevalidateShardPathResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodePrevalidateShardPathRequest newNodeRequest(PrevalidateShardPathRequest request) {
        return new NodePrevalidateShardPathRequest(request.getShardIds());
    }

    @Override
    protected NodePrevalidateShardPathResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodePrevalidateShardPathResponse(in);
    }

    @Override
    protected NodePrevalidateShardPathResponse nodeOperation(NodePrevalidateShardPathRequest request, Task task) {
        Set<ShardId> localShards = new HashSet<>();
        ShardPath shardPath = null;
        // For each shard we only check whether the shard path exists, regardless of whether the content is a valid index or not.
        for (ShardId shardId : request.getShardIds()) {
            try {
                var indexMetadata = clusterService.state().metadata().index(shardId.getIndex());
                String customDataPath = null;
                if (indexMetadata != null) {
                    customDataPath = new IndexSettings(indexMetadata, settings).customDataPath();
                } else {
                    // The index is not known to this node. This shouldn't happen, but it can be safely ignored for this operation.
                    logger.warn("node doesn't have metadata for the index [{}]", shardId.getIndex());
                }
                shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
                if (shardPath != null) {
                    localShards.add(shardId);
                }
            } catch (IOException e) {
                final String path = shardPath != null ? shardPath.resolveIndex().toString() : "";
                logger.error(() -> String.format(Locale.ROOT, "error loading shard path for shard [%s]", shardId), e);
            }
        }
        return new NodePrevalidateShardPathResponse(transportService.getLocalNode(), localShards);
    }
}
