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

package co.elastic.elasticsearch.stateless.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

public class TransportSendRecoveryCommitRegistrationAction extends HandledTransportAction<RegisterCommitRequest, RegisterCommitResponse> {

    public static final String NAME = "internal:index/shard/recovery:send_recovery_commit_registration";
    public static final ActionType<RegisterCommitResponse> TYPE = new ActionType<>(NAME, RegisterCommitResponse::new);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;

    @Inject
    public TransportSendRecoveryCommitRegistrationAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(NAME, transportService, actionFilters, RegisterCommitRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, RegisterCommitRequest request, ActionListener<RegisterCommitResponse> listener) {
        var state = clusterService.state();
        var shardId = request.getShardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        assert indexShard.routingEntry().isPromotableToPrimary() == false
            : "TransportRouteRecoveryCommitRegistrationAction can only be executed on a search shard";
        // Forward the request to the indexing shard
        var shardRoutingTable = state.routingTable().shardRoutingTable(shardId);
        if (shardRoutingTable.primaryShard() == null || shardRoutingTable.primaryShard().active() == false) {
            // TODO: A search shard should be able to continue using the found commit despite not being able to register it.
            // For now, we fail the request to register the commit.
            throw new NoShardAvailableActionException(shardId, "primary shard is not active");
        }
        DiscoveryNode node = state.nodes().get(shardRoutingTable.primaryShard().currentNodeId());
        assert node != null;
        // TODO: retry on ShardNotFoundException (with a new cluster state)
        transportService.sendChildRequest(
            node,
            TransportRegisterCommitForRecoveryAction.NAME,
            new RegisterCommitRequest(request.getCommit(), request.getShardId(), request.getNodeId(), state.version()),
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, RegisterCommitResponse::new, transportService.getThreadPool().generic())
        );
    }
}
