/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger logger = LogManager.getLogger(TransportPutDataFrameJobAction.class);

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

        String jobId = request.getConfig().getId();
        // quick check whether a job has already been created under that name
        if (PersistentTasksCustomMetaData.getTaskWithId(clusterState, jobId) != null) {
            listener.onFailure(new ResourceAlreadyExistsException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_JOB_EXISTS, jobId)));
            return;
        }

        // create the job, note the non-state creating steps are done first, so we minimize the chance to end up with orphaned state
        // job validation
        JobValidator jobCreator = new JobValidator(request.getConfig(), client);
        jobCreator.validate(ActionListener.wrap(validationResult -> {
            // deduce target mappings
            jobCreator.deduceMappings(ActionListener.wrap(mappings -> {
                // create the destination index
                DataframeIndex.createDestinationIndex(client, request.getConfig(), mappings, ActionListener.wrap(createIndexResult -> {
                    DataFrameJob job = createDataFrameJob(jobId, threadPool);
                    // create the job configuration and store it in the internal index
                    dataFrameJobConfigManager.putJobConfiguration(request.getConfig(), false, ActionListener.wrap(r -> {
                        // finally start the persistent task
                        persistentTasksService.sendStartRequest(job.getId(), DataFrameJob.NAME, job, ActionListener.wrap(persistentTask -> {
                            listener.onResponse(new PutDataFrameJobAction.Response(true));
                        }, startPersistentTaskException -> {
                            // delete the otherwise orphaned job configuration, for now we do not delete the destination index
                            dataFrameJobConfigManager.deleteJobConfiguration(jobId, ActionListener.wrap(r2 -> {
                                logger.debug("Deleted data frame job [{}] configuration from data frame configuration index", jobId);
                                listener.onFailure(
                                        new RuntimeException(
                                                DataFrameMessages.getMessage(
                                                        DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_START_PERSISTENT_TASK, r2),
                                                startPersistentTaskException));
                            }, deleteJobFromIndexException -> {
                                logger.error("Failed to cleanup orphaned data frame job [{}] configuration", jobId);
                                listener.onFailure(
                                        new RuntimeException(
                                                DataFrameMessages.getMessage(
                                                        DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_START_PERSISTENT_TASK, false),
                                                startPersistentTaskException));
                            }));
                        }));
                    }, pubJobIntoIndexException -> {
                        if (pubJobIntoIndexException instanceof VersionConflictEngineException) {
                            // the job already exists although we checked before, can happen if requests come in simultaneously
                            listener.onFailure(new ResourceAlreadyExistsException(DataFrameMessages
                                    .getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_JOB_EXISTS, jobId)));
                        } else {
                            listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_PERSIST_JOB_CONFIGURATION,
                                    pubJobIntoIndexException));
                        }
                    }));
                }, createDestinationIndexException -> {
                    listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_CREATE_TARGET_INDEX,
                            createDestinationIndexException));
                }));
            }, deduceTargetMappingsException -> {
                listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_DEDUCE_TARGET_MAPPINGS,
                        deduceTargetMappingsException));
            }));
        }, validationException -> {
            listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                    validationException));
        }));
    }

    private static DataFrameJob createDataFrameJob(String jobId, ThreadPool threadPool) {
        return new DataFrameJob(jobId);
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
