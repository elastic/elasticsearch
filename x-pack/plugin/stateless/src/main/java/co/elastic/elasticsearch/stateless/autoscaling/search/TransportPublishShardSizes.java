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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPublishShardSizes extends TransportMasterNodeAction<PublishShardSizesRequest, ActionResponse.Empty> {

    public static final String NAME = "cluster:monitor/stateless/autoscaling/push_shard_sizes";
    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>(NAME);

    private final SearchMetricsService searchTierMetricsService;

    @Inject
    public TransportPublishShardSizes(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchMetricsService searchTierMetricsService
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PublishShardSizesRequest::new,
            indexNameExpressionResolver,
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.searchTierMetricsService = searchTierMetricsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PublishShardSizesRequest request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            searchTierMetricsService.processShardSizesRequest(request);
            return ActionResponse.Empty.INSTANCE;
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PublishShardSizesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
