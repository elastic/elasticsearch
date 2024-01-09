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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPublishNodeIngestLoadMetric extends TransportMasterNodeAction<PublishNodeIngestLoadRequest, ActionResponse.Empty> {

    public static final String NAME = "cluster:monitor/stateless/autoscaling/push_node_ingest_load";
    public static final ActionType<ActionResponse.Empty> INSTANCE = ActionType.localOnly(NAME);

    private final IngestMetricsService ingestMetricsService;

    @Inject
    public TransportPublishNodeIngestLoadMetric(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IngestMetricsService ingestMetricsService
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PublishNodeIngestLoadRequest::new,
            indexNameExpressionResolver,
            in -> ActionResponse.Empty.INSTANCE,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.ingestMetricsService = ingestMetricsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PublishNodeIngestLoadRequest request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            ingestMetricsService.trackNodeIngestLoad(request.getNodeId(), request.getSeqNo(), request.getIngestionLoad());
            return ActionResponse.Empty.INSTANCE;
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PublishNodeIngestLoadRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
