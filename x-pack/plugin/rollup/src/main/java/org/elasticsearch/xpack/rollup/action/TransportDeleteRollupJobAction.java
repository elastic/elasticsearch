/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TransportDeleteRollupJobAction
        extends TransportMasterNodeAction<DeleteRollupJobAction.Request, AcknowledgedResponse> {

    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportDeleteRollupJobAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          PersistentTasksService persistentTasksService, ClusterService clusterService) {
        super(settings, DeleteRollupJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, DeleteRollupJobAction.Request::new);
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(DeleteRollupJobAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {

        String jobId = request.getId();
        TimeValue timeout = new TimeValue(60, TimeUnit.SECONDS); // TODO make this a config option

        // Step 1. Cancel the persistent task
        persistentTasksService.sendRemoveRequest(jobId, new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
            @Override
            public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                logger.debug("Request to cancel Task for Rollup job [" + jobId + "] successful.");

                // Step 2. Wait for the task to finish cancellation internally
                persistentTasksService.waitForPersistentTaskCondition(jobId, Objects::isNull, timeout,
                        new PersistentTasksService.WaitForPersistentTaskListener<RollupJob>() {
                            @Override
                            public void onResponse(PersistentTasksCustomMetaData.PersistentTask<RollupJob> task) {
                                logger.debug("Task for Rollup job [" + jobId + "] successfully canceled.");
                                listener.onResponse(new AcknowledgedResponse(true));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.error("Error while cancelling task for Rollup job [" + jobId
                                        + "]." + e);
                                listener.onFailure(e);
                            }

                            @Override
                            public void onTimeout(TimeValue timeout) {
                                String msg = "Stopping of Rollup job [" + jobId + "] timed out after [" + timeout + "].";
                                logger.warn(msg);
                                listener.onFailure(new ElasticsearchException(msg));
                            }
                        });
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Error while requesting to cancel task for Rollup job [" + jobId
                        + "]" + e);
                listener.onFailure(e);
            }
        });

    }

    @Override
    protected ClusterBlockException checkBlock(DeleteRollupJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
