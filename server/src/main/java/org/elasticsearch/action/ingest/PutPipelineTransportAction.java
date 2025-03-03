/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoMetrics;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.ingest.IngestService.INGEST_ORIGIN;

public class PutPipelineTransportAction extends AcknowledgedTransportMasterNodeAction<PutPipelineRequest> {
    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("cluster:admin/ingest/pipeline/put");
    private final IngestService ingestService;
    private final OriginSettingClient client;
    private final ProjectResolver projectResolver;

    @Inject
    public PutPipelineTransportAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IngestService ingestService,
        NodeClient client
    ) {
        super(
            TYPE.name(),
            transportService,
            ingestService.getClusterService(),
            threadPool,
            actionFilters,
            PutPipelineRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        // This client is only used to perform an internal implementation detail,
        // so uses an internal origin context rather than the user context
        this.client = new OriginSettingClient(client, INGEST_ORIGIN);
        this.ingestService = ingestService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(Task task, PutPipelineRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        ingestService.putPipeline(projectResolver.getProjectId(), request, listener, (nodeListener) -> {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
            nodesInfoRequest.clear();
            nodesInfoRequest.addMetric(NodesInfoMetrics.Metric.INGEST.metricName());
            client.admin().cluster().nodesInfo(nodesInfoRequest, nodeListener);
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PutPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedPipelineAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(PutPipelineRequest request) {
        return Set.of(request.getId());
    }

    @Override
    protected void validateForReservedState(PutPipelineRequest request, ClusterState state) {
        super.validateForReservedState(request, state);

        validateForReservedState(
            projectResolver.getProjectMetadata(state).reservedStateMetadata().values(),
            reservedStateHandlerName().get(),
            modifiedKeys(request),
            request.toString()
        );
    }
}
