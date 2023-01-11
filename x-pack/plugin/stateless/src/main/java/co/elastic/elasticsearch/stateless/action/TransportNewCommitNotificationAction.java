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

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.engine.SearchEngine;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportNewCommitNotificationAction extends HandledTransportAction<NewCommitNotificationRequest, ActionResponse.Empty> {
    private static final Logger logger = LogManager.getLogger(TransportNewCommitNotificationAction.class);
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final Client client;

    @Inject
    public TransportNewCommitNotificationAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        Client client
    ) {
        super(NewCommitNotificationAction.NAME, transportService, actionFilters, NewCommitNotificationRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, NewCommitNotificationRequest request, ActionListener<ActionResponse.Empty> listener) {
        if (request.isIndexingShard()) {
            // Forward the request to all nodes that hold search/replica shards
            final ClusterState clusterState = clusterService.state();
            clusterState.routingTable()
                .shardRoutingTable(request.getShardId())
                .replicaShards()
                .stream()
                .filter(replicaShard -> replicaShard.started())
                .forEach(replicaShard -> {
                    assert replicaShard.primary() == false;
                    final DiscoveryNode node = clusterState.nodes().get(replicaShard.currentNodeId());
                    logger.debug("forwarding notify request [{}] to replica shard [{}]", request, replicaShard);
                    transportService.sendChildRequest(
                        node,
                        NewCommitNotificationAction.NAME,
                        request.withIndexingShard(false),
                        task,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(ActionListener.noop(), (in) -> TransportResponse.Empty.INSTANCE)
                    ); // TODO Handle failed notifications if necessary
                });
        } else {
            logger.debug("received notify request [{}]", request);
            IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
            Engine engineOrNull = shard.getEngineOrNull();
            if (engineOrNull == null) {
                throw new EngineException(shard.shardId(), "Engine not started.");
            }
            if (engineOrNull instanceof SearchEngine searchEngine) {
                try {
                    searchEngine.onCommitNotification(
                        request.getTerm(),
                        request.getGeneration(),
                        request.getFiles(),
                        ActionListener.noop()
                    );
                } catch (IOException e) {
                    // TODO: Consider if we want to improve exception handling or propagate this exception
                    logger.error("Exception attempting to load new commit.", e);
                }
            } else {
                throw new ElasticsearchException("Engine not type SearchEngine.");
            }
        }
        // TODO: Consider if we want to wait for commit reload
        listener.onResponse(ActionResponse.Empty.INSTANCE);
    }
}
