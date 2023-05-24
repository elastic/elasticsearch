/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.action;

import co.elastic.elasticsearch.stateless.autoscaling.action.GetStatelessAutoscalingMetricsAction.Request;
import co.elastic.elasticsearch.stateless.autoscaling.action.GetStatelessAutoscalingMetricsAction.Response;
import co.elastic.elasticsearch.stateless.autoscaling.model.ConstraintsContainer;
import co.elastic.elasticsearch.stateless.autoscaling.model.ConstraintsContainer.NodeLevelConstraints;
import co.elastic.elasticsearch.stateless.autoscaling.model.ConstraintsContainer.TierLevelConstraints;
import co.elastic.elasticsearch.stateless.autoscaling.model.MetricsContainer;
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

    @Inject
    public TransportGetStatelessAutoscalingMetricsAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver
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
    }

    @Override
    protected void masterOperation(
        final Task task,
        final Request request,
        final ClusterState state,
        final ActionListener<Response> listener
    ) {

        final TierMetrics indexTierMetrics = new TierMetrics.IndexTierMetrics(
            new MetricsContainer(Map.of("load", 100)),
            new ConstraintsContainer(
                new TierLevelConstraints(Map.of("min_num_nodes", 2, "min_heap_bytes", 2000)),
                new NodeLevelConstraints(Map.of("min_heap_bytes", 2000, "min_storage_bytes", 1000))
            )
        );

        final TierMetrics searchTierMetrics = new TierMetrics.SearchTierMetrics(
            new MetricsContainer(Map.of("interactive_load", 10, "non_interactive_load", 0)),
            new ConstraintsContainer(
                new TierLevelConstraints(Map.of("min_num_nodes", 2, "min_heap_bytes", 2000000)),
                new NodeLevelConstraints(Map.of("min_heap_bytes", 2000, "min_storage_bytes", 2000000))
            )
        );

        final StatelessAutoscalingMetrics metrics = new StatelessAutoscalingMetrics(indexTierMetrics, searchTierMetrics);

        listener.onResponse(new Response(metrics));

    }

    @Override
    protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
