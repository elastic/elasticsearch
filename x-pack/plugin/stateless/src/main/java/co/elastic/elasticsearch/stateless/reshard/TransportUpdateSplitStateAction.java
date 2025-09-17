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

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportUpdateSplitStateAction extends TransportMasterNodeAction<SplitStateRequest, ActionResponse.Empty> {

    public static final ActionType<ActionResponse> TYPE = new ActionType<>("indices:admin/reshard/split_state");

    private final ReshardIndexService reshardIndexService;

    @Inject
    public TransportUpdateSplitStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ReshardIndexService reshardIndexService,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            transportService.getThreadPool(),
            actionFilters,
            SplitStateRequest::new,
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.reshardIndexService = reshardIndexService;
    }

    @Override
    protected void masterOperation(Task task, SplitStateRequest request, ClusterState state, ActionListener<ActionResponse.Empty> listener)
        throws Exception {
        if (request.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.HANDOFF) {
            reshardIndexService.transitionToHandoff(request, listener.map(ignored -> ActionResponse.Empty.INSTANCE));
        } else {
            assert request.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT
                || request.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.DONE;
            reshardIndexService.transitionTargetState(request, listener.map(ignored -> ActionResponse.Empty.INSTANCE));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(SplitStateRequest request, ClusterState state) {
        ShardId shardId = request.getShardId();
        final ProjectMetadata project = state.metadata().lookupProject(shardId.getIndex()).get();
        return state.blocks().indexBlockedException(project.id(), ClusterBlockLevel.METADATA_WRITE, shardId.getIndex().getName());
    }
}
