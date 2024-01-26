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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

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

public class TransportPublishHeapMemoryMetrics extends TransportMasterNodeAction<PublishHeapMemoryMetricsRequest, ActionResponse.Empty> {

    public static final String NAME = "cluster:monitor/stateless/autoscaling/publish_heap_memory_metrics";
    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>(NAME);

    private final MemoryMetricsService memoryMetricsService;

    @Inject
    public TransportPublishHeapMemoryMetrics(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final MemoryMetricsService memoryMetricsService
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PublishHeapMemoryMetricsRequest::new,
            indexNameExpressionResolver,
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.memoryMetricsService = memoryMetricsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PublishHeapMemoryMetricsRequest request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            memoryMetricsService.updateIndicesMappingSize(request.getHeapMemoryUsage());
            return ActionResponse.Empty.INSTANCE;
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PublishHeapMemoryMetricsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
