/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 * Transport action for retrieving sampling configuration for an index.
 * <p>
 * This action retrieves the sampling configuration from the project metadata's custom sampling metadata.
 * If no configuration exists for the specified index, a response with null configuration is returned.
 * </p>
 */
public class TransportGetSampleConfigurationAction extends TransportLocalProjectMetadataAction<
    GetSampleConfigurationAction.Request,
    GetSampleConfigurationAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetSampleConfigurationAction.class);
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * Constructs a new TransportGetSampleConfigurationAction.
     *
     * @param actionFilters the action filters to apply
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param projectResolver the project resolver
     * @param indexNameExpressionResolver the index name expression resolver for resolving index names
     */
    @Inject
    public TransportGetSampleConfigurationAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSampleConfigurationAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;

    }

    /**
     * Executes the get sampling configuration operation against the local cluster state.
     * <p>
     * Retrieves the sampling configuration for the specified index from the project metadata.
     * Returns null configuration if no sampling metadata exists or if no configuration is set for the index.
     * </p>
     *
     * @param task the task executing this operation
     * @param request the get sampling configuration request containing the index name
     * @param state the current project state
     * @param listener the listener to notify with the response or failure
     */
    @Override
    protected void localClusterStateOperation(
        Task task,
        GetSampleConfigurationAction.Request request,
        ProjectState state,
        ActionListener<GetSampleConfigurationAction.Response> listener
    ) {
        ProjectMetadata projectMetadata = state.metadata();

        // throws IndexNotFoundException if any index does not exist or more than one index is resolved
        try {
            indexNameExpressionResolver.concreteIndexNames(projectMetadata, request);
        } catch (IndexNotFoundException e) {
            logger.debug("Index not found: [{}]", request.getIndex());
            listener.onFailure(e);
            return;
        }
        if (projectMetadata == null) {
            logger.debug("No project metadata found for index [{}]", request.getIndex());
            listener.onResponse(new GetSampleConfigurationAction.Response(request.getIndex(), null));
            return;
        }

        SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
        if (samplingMetadata == null) {
            logger.debug("No sampling metadata found for index [{}]", request.getIndex());
            listener.onResponse(new GetSampleConfigurationAction.Response(request.getIndex(), null));
            return;
        }

        Map<String, SamplingConfiguration> indexToSampleConfigMap = samplingMetadata.getIndexToSamplingConfigMap();
        SamplingConfiguration config = indexToSampleConfigMap.get(request.getIndex());
        if (config == null) {
            logger.debug("No sampling configuration found for index [{}]", request.getIndex());
            listener.onResponse(new GetSampleConfigurationAction.Response(request.getIndex(), config));
            return;
        }

        logger.debug("Retrieved sampling configuration for index [{}]", request.getIndex());
        listener.onResponse(new GetSampleConfigurationAction.Response(request.getIndex(), config));
    }

    /**
     * Checks for cluster blocks that would prevent this operation from executing.
     *
     * @param request the get sampling configuration request
     * @param state the current project state
     * @return a cluster block exception if the operation is blocked, null otherwise
     */
    @Override
    protected ClusterBlockException checkBlock(GetSampleConfigurationAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
