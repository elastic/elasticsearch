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

import static co.elastic.elasticsearch.stateless.autoscaling.model.Metric.array;
import static co.elastic.elasticsearch.stateless.autoscaling.model.Metric.exact;

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
        listener.onResponse(
            new Response(
                new StatelessAutoscalingMetrics(
                    Map.of(
                        "index",
                        new TierMetrics(
                            Map.ofEntries(
                                Map.entry("node_memory_in_bytes", exact(4096)),
                                Map.entry("total_memory_in_bytes", exact(8192)),
                                Map.entry("indexing_load", array(exact(4), exact(10), exact(20)))
                            )
                        ),
                        "search",
                        new TierMetrics(
                            Map.ofEntries(
                                Map.entry("node_memory_in_bytes", exact(4096)),
                                Map.entry("total_memory_in_bytes", exact(8192)),
                                Map.entry("max_shard_copies", exact(1)),
                                Map.entry("max_interactive_data_size_in_bytes", exact(100)),
                                Map.entry("total_interactive_data_size_in_bytes", exact(1000)),
                                Map.entry("total_data_size_in_bytes", exact(10000))
                            )
                        )
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
