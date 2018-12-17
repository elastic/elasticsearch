/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameJobAction.Request;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameJobAction.Response;
import org.elasticsearch.xpack.dataframe.job.DataFrameJob;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameJobConfigManager;
import org.elasticsearch.xpack.dataframe.persistence.DataframeIndex;
import org.elasticsearch.xpack.dataframe.support.JobValidator;

public class TransportPutDataFrameJobAction
        extends TransportMasterNodeAction<PutDataFrameJobAction.Request, PutDataFrameJobAction.Response> {

    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final Client client;
    private final DataFrameJobConfigManager dataFrameJobConfigManager;

    @Inject
    public TransportPutDataFrameJobAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, XPackLicenseState licenseState,
            PersistentTasksService persistentTasksService, DataFrameJobConfigManager dataFrameJobConfigManager, Client client) {
        super(PutDataFrameJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, PutDataFrameJobAction.Request::new);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
        this.dataFrameJobConfigManager = dataFrameJobConfigManager;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDataFrameJobAction.Response newResponse() {
        return new PutDataFrameJobAction.Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState clusterState, ActionListener<Response> listener) throws Exception {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        // quick check whether a job has already been created under that name
        if (PersistentTasksCustomMetaData.getTaskWithId(clusterState, request.getConfig().getId()) != null) {
            listener.onFailure(new ResourceAlreadyExistsException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_JOB_EXISTS, request.getConfig().getId())));
            return;
        }

        // job validation
        JobValidator jobCreator = new JobValidator(request.getConfig(), client);
        jobCreator.validate(ActionListener.wrap(validationResult -> {
            dataFrameJobConfigManager.putJobConfiguration(request.getConfig(), false, ActionListener.wrap(r -> {
                jobCreator.deduceMappings(ActionListener.wrap(mappings -> {
                    DataFrameJob job = createDataFrameJob(request.getConfig().getId(), threadPool);
                    DataframeIndex.createDestinationIndex(client, request.getConfig(), mappings, ActionListener.wrap(createIndexResult -> {
                        startPersistentTask(job, listener, persistentTasksService);
                    }, persistentTaskException -> {
                        // todo: cleanup job in index, or change order
                        listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_CREATE_TARGET_INDEX,
                                persistentTaskException));
                    }));
                }, deduceTargetMappingsException -> {
                    // todo: cleanup job in index or change order
                    listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_DEDUCE_TARGET_MAPPINGS,
                            deduceTargetMappingsException));
                }));
            }, pubJobIntoIndexException -> {
                if (pubJobIntoIndexException instanceof VersionConflictEngineException) {
                    // the job already exists although we checked before, can happen if requests come in simultaneously
                    listener.onFailure(new ResourceAlreadyExistsException(
                            DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_JOB_EXISTS, request.getConfig().getId())));
                } else {
                    listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_PERSIST_JOB_CONFIGURATION,
                            pubJobIntoIndexException));
                }
            }));
        }, validationException -> {
            listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                    validationException));
        }));
    }

    private static DataFrameJob createDataFrameJob(String jobId, ThreadPool threadPool) {
        return new DataFrameJob(jobId);
    }

    static void startPersistentTask(DataFrameJob job, ActionListener<PutDataFrameJobAction.Response> listener,
            PersistentTasksService persistentTasksService) {

        persistentTasksService.sendStartRequest(job.getId(), DataFrameJob.NAME, job,
                ActionListener.wrap(persistentTask -> {
                    listener.onResponse(new PutDataFrameJobAction.Response(true));
                }, e -> {
                    listener.onFailure(e);
                }));
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
