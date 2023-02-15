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
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

public class TransportNewCommitNotificationAction extends HandledTransportAction<NewCommitNotificationRequest, ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportNewCommitNotificationAction.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;

    @Inject
    public TransportNewCommitNotificationAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(
            NewCommitNotificationAction.NAME,
            transportService,
            actionFilters,
            NewCommitNotificationRequest::new,
            ThreadPool.Names.REFRESH
        );
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, NewCommitNotificationRequest request, ActionListener<ActionResponse.Empty> responseListener) {
        if (request.isIndexingShard()) {
            try (var listeners = new RefCountingListener(responseListener.map(v -> ActionResponse.Empty.INSTANCE))) {
                // Forward the request to all nodes that hold search shards
                NewCommitNotificationRequest forwardRequest = request.withIndexingShard(false);
                final ClusterState clusterState = clusterService.state();
                clusterState.routingTable()
                    .shardRoutingTable(request.getShardId())
                    .activeShards()
                    .stream()
                    .filter(ShardRouting::isSearchable)
                    .map(ShardRouting::currentNodeId)
                    .forEach(nodeId -> {
                        final DiscoveryNode node = clusterState.nodes().get(nodeId);
                        logRequest("forwarding to " + node.descriptionWithoutAttributes() + " the", request);
                        transportService.sendChildRequest(
                            node,
                            NewCommitNotificationAction.NAME,
                            forwardRequest,
                            task,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(listeners.acquire(ignored -> {}), (in) -> TransportResponse.Empty.INSTANCE)
                        );
                    });
            } catch (Exception e) {
                responseListener.onFailure(e);
            }
        } else {
            ActionListener.run(responseListener, (listener) -> {
                logRequest("received", request);
                IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
                shard.readAllowed();
                Engine engineOrNull = shard.getEngineOrNull();
                if (engineOrNull == null) {
                    throw new EngineException(shard.shardId(), "Engine not started.");
                }
                if (engineOrNull instanceof SearchEngine searchEngine) {
                    searchEngine.onCommitNotification(
                        request.getTerm(),
                        request.getGeneration(),
                        request.getFiles(),
                        listener.map(ignored -> ActionResponse.Empty.INSTANCE)
                    );
                } else {
                    assert false : "expecting SearchEngine but got " + engineOrNull;
                    throw new ElasticsearchException("Engine not type SearchEngine.");
                }
            });
        }
    }

    private static void logRequest(String prefix, NewCommitNotificationRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("{} new commit notification request [{}]", prefix, request);
        } else {
            logger.debug(
                "{} new commit notification request for shard [{}], term [{}], generation [{}]",
                prefix,
                request.getShardId(),
                request.getTerm(),
                request.getGeneration()
            );
        }
    }
}
