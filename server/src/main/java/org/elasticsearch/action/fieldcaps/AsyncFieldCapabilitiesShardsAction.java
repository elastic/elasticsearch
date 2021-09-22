/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * An action that executes on each shard sequentially until it finds one that can match the provided
 * {@link FieldCapabilitiesIndexRequest#indexFilter()}. In which case the shard is used
 * to create the final {@link FieldCapabilitiesIndexResponse}.
 */
class AsyncFieldCapabilitiesShardsAction {
    private static final Logger logger = LogManager.getLogger(AsyncFieldCapabilitiesShardsAction.class);

    private final FieldCapabilitiesIndexRequest request;
    private final TransportService transportService;
    private final DiscoveryNodes nodes;
    private final ActionListener<FieldCapabilitiesIndexResponse> listener;
    private final GroupShardsIterator<ShardIterator> shardsIt;

    private volatile int shardIndex = 0;

    AsyncFieldCapabilitiesShardsAction(TransportService transportService,
                                       ClusterService clusterService,
                                       FieldCapabilitiesIndexRequest request,
                                       ActionListener<FieldCapabilitiesIndexResponse> listener) {
        this.listener = listener;
        this.transportService = transportService;

        ClusterState clusterState = clusterService.state();
        if (logger.isTraceEnabled()) {
            logger.trace("executing [{}] based on cluster state version [{}]", request,
                clusterState.version());
        }
        nodes = clusterState.nodes();
        this.request = request;

        shardsIt = clusterService.operationRouting().searchShards(clusterService.state(),
            new String[]{request.index()}, null, null, null, null);
    }

    public void start() {
        tryNext(null, true);
    }

    private void onFailure(ShardRouting shardRouting, Exception e) {
        if (e != null) {
            logger.trace(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting,
                request), e);
        }
        tryNext(e, false);
    }

    private ShardRouting nextRoutingOrNull() {
        if (shardsIt.size() == 0 || shardIndex >= shardsIt.size()) {
            return null;
        }
        ShardRouting next = shardsIt.get(shardIndex).nextOrNull();
        if (next != null) {
            return next;
        }
        moveToNextShard();
        return nextRoutingOrNull();
    }

    private void moveToNextShard() {
        ++shardIndex;
    }

    private void tryNext(@Nullable final Exception lastFailure, boolean canMatchShard) {
        ShardRouting shardRouting = nextRoutingOrNull();
        if (shardRouting == null) {
            if (canMatchShard == false) {
                if (lastFailure == null) {
                    listener.onResponse(new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false));
                } else {
                    logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null,
                        request), lastFailure);
                    listener.onFailure(lastFailure);
                }
            } else {
                if (lastFailure == null || isShardNotAvailableException(lastFailure)) {
                    listener.onFailure(new NoShardAvailableActionException(null,
                        LoggerMessageFormat.format("No shard available for [{}]", request), lastFailure));
                } else {
                    logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null,
                        request), lastFailure);
                    listener.onFailure(lastFailure);
                }
            }
            return;
        }
        DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
        if (node == null) {
            onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
        } else {
            request.shardId(shardRouting.shardId());
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "sending request [{}] on node [{}]",
                    request,
                    node
                );
            }
            transportService.sendRequest(node, TransportFieldCapabilitiesAction.ACTION_SHARD_NAME, request,
                new TransportResponseHandler<FieldCapabilitiesIndexResponse>() {

                    @Override
                    public FieldCapabilitiesIndexResponse read(StreamInput in) throws IOException {
                        return new FieldCapabilitiesIndexResponse(in);
                    }

                    @Override
                    public void handleResponse(final FieldCapabilitiesIndexResponse response) {
                        if (response.canMatch()) {
                            listener.onResponse(response);
                        } else {
                            moveToNextShard();
                            tryNext(null, false);
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        onFailure(shardRouting, exp);
                    }
                });
        }
    }
}
