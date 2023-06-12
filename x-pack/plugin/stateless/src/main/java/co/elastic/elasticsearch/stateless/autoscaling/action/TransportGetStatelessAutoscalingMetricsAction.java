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

package co.elastic.elasticsearch.stateless.autoscaling.action;

import co.elastic.elasticsearch.stateless.autoscaling.action.GetStatelessAutoscalingMetricsAction.Request;
import co.elastic.elasticsearch.stateless.autoscaling.action.GetStatelessAutoscalingMetricsAction.Response;
import co.elastic.elasticsearch.stateless.autoscaling.action.metrics.AutoscalingDiskSizeMetricsService;
import co.elastic.elasticsearch.stateless.autoscaling.model.StatelessAutoscalingMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.model.TierMetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 * Transport for GetStatelessAutoscalingMetricsAction. Entrance point for calculation of stateless autoscaling metrics
 */
public class TransportGetStatelessAutoscalingMetricsAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetStatelessAutoscalingMetricsAction.class);

    private final AutoscalingDiskSizeMetricsService autoscalingDiskSizeMetricsService;

    @Inject
    public TransportGetStatelessAutoscalingMetricsAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AutoscalingDiskSizeMetricsService autoscalingDiskSizeMetricsService
    ) {
        super(
            GetStatelessAutoscalingMetricsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.autoscalingDiskSizeMetricsService = autoscalingDiskSizeMetricsService;
    }

    @Override
    protected void masterOperation(
        final Task task,
        final Request request,
        final ClusterState state,
        final ActionListener<Response> listener
    ) {
        listener.onResponse(
            new Response(
                new StatelessAutoscalingMetrics(
                    Map.of(
                        "index-tier",
                        new TierMetrics(
                            Map.ofEntries(Map.entry("indexing_load", new int[] { 4, 10, 20 }), Map.entry("min_memory_in_bytes", 1000))
                        ),
                        "search-tier",
                        new TierMetrics(Map.ofEntries(Map.entry("interactive_load", 10), Map.entry("non_interactive_load", 0)))
                    )
                )
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
