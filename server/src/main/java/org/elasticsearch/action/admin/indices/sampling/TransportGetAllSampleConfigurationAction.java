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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 * Transport action for retrieving all existing sampling configurations.
 * <p>
 * This action retrieves the sampling configurations from the project metadata's custom sampling metadata.
 * </p>
 */
public class TransportGetAllSampleConfigurationAction extends TransportLocalProjectMetadataAction<
    GetAllSampleConfigurationAction.Request,
    GetAllSampleConfigurationAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetAllSampleConfigurationAction.class);

    /**
     * Constructs a new TransportGetAllSampleConfigurationAction.
     *
     * @param actionFilters the action filters to apply
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param projectResolver the project resolver
     */
    @Inject
    public TransportGetAllSampleConfigurationAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(
            GetAllSampleConfigurationAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
    }

    /**
     * Executes the get all sampling configuration operation against the local cluster state.
     * <p>
     * Retrieves all sampling configuration from the project metadata.
     * Returns empty map if no sampling metadata exists or if no configurations are set.
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
        GetAllSampleConfigurationAction.Request request,
        ProjectState state,
        ActionListener<GetAllSampleConfigurationAction.Response> listener
    ) {
        ProjectMetadata projectMetadata = state.metadata();

        if (projectMetadata == null) {
            logger.debug("No project metadata found");
            listener.onResponse(new GetAllSampleConfigurationAction.Response(Map.of()));
            return;
        }

        SamplingMetadata samplingMetadata = projectMetadata.custom(SamplingMetadata.TYPE);
        if (samplingMetadata == null) {
            logger.debug("No sampling metadata found");
            listener.onResponse(new GetAllSampleConfigurationAction.Response(Map.of()));
            return;
        }

        Map<String, SamplingConfiguration> indexToSampleConfigMap = samplingMetadata.getIndexToSamplingConfigMap();
        logger.debug("Retrieved sampling configurations");
        listener.onResponse(
            new GetAllSampleConfigurationAction.Response(indexToSampleConfigMap == null ? Map.of() : indexToSampleConfigMap)
        );
    }

    /**
     * Checks for cluster blocks that would prevent this operation from executing.
     *
     * @param request the get sampling configuration request
     * @param state the current project state
     * @return a cluster block exception if the operation is blocked, null otherwise
     */
    @Override
    protected ClusterBlockException checkBlock(GetAllSampleConfigurationAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
