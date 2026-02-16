/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for updating sampling configurations in cluster metadata.
 * <p>
 * This action handles the cluster state update required to store sampling configurations
 * for the specified indices. It validates the request, resolves index names, and updates
 * the cluster metadata with the new sampling configuration.
 * </p>
 */
public class TransportPutSampleConfigurationAction extends AcknowledgedTransportMasterNodeAction<PutSampleConfigurationAction.Request> {
    private static final Logger logger = LogManager.getLogger(TransportPutSampleConfigurationAction.class);
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SamplingService samplingService;

    @Inject
    public TransportPutSampleConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SamplingService samplingService
    ) {
        super(
            PutSampleConfigurationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutSampleConfigurationAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.samplingService = samplingService;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutSampleConfigurationAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        try {
            SamplingService.throwIndexNotFoundExceptionIfNotDataStreamOrIndex(indexNameExpressionResolver, projectResolver, state, request);
        } catch (IndexNotFoundException e) {
            listener.onFailure(e);
            return;
        }

        ProjectId projectId = projectResolver.getProjectId();
        samplingService.updateSampleConfiguration(
            projectId,
            request.indices()[0],
            request.getSampleConfiguration(),
            request.masterNodeTimeout(),
            request.ackTimeout(),
            listener
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PutSampleConfigurationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

}
