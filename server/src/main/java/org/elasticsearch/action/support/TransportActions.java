/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

public class TransportActions {

    public static boolean isShardNotAvailableException(final Throwable e) {
        final Throwable actual = ExceptionsHelper.unwrapCause(e);
        return (actual instanceof ShardNotFoundException
            || actual instanceof IndexNotFoundException
            || actual instanceof IllegalIndexShardStateException
            || actual instanceof NoShardAvailableActionException
            || actual instanceof UnavailableShardsException
            || actual instanceof AlreadyClosedException);
    }

    /**
     * If a failure is already present, should this failure override it or not for read operations.
     */
    public static boolean isReadOverrideException(Exception e) {
        return isShardNotAvailableException(e) == false;
    }

    public static void broadcastToUnpromotableShards(
        ShardId shardId,
        ActionRequest request,
        String action,
        Task parentTaskId,
        ClusterService clusterService,
        TransportService transportService,
        String executor,
        ActionListener<Void> listener
    ) {
        try (var listeners = new RefCountingListener(listener.map(v -> null))) {
            final ClusterState clusterState = clusterService.state();
            final DiscoveryNodes nodes = clusterState.nodes();
            clusterState.routingTable().shardRoutingTable(shardId).unpromotableShards().forEach(sr -> {
                final DiscoveryNode node = nodes.get(sr.currentNodeId());
                transportService.sendChildRequest(
                    node,
                    action,
                    request,
                    parentTaskId,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        listeners.acquire(ignored -> {}),
                        (in) -> TransportResponse.Empty.INSTANCE,
                        executor
                    )
                );
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
