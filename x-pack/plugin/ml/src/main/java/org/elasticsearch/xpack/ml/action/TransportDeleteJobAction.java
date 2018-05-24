/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MLMetadataField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.job.persistence.JobStorageDeletionTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.xpack.ml.job.JobManager;

import java.util.concurrent.TimeoutException;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteJobAction extends TransportMasterNodeAction<DeleteJobAction.Request, DeleteJobAction.Response> {

    private final Client client;
    private final JobManager jobManager;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportDeleteJobAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    ThreadPool threadPool, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver, JobManager jobManager,
                                    PersistentTasksService persistentTasksService, Client client) {
        super(settings, DeleteJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, DeleteJobAction.Request::new);
        this.client = client;
        this.jobManager = jobManager;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteJobAction.Response newResponse() {
        return new DeleteJobAction.Response();
    }

    @Override
    protected void masterOperation(Task task, DeleteJobAction.Request request, ClusterState state,
                                   ActionListener<DeleteJobAction.Response> listener) throws Exception {

        ActionListener<Boolean> markAsDeletingListener = ActionListener.wrap(
                response -> {
                    if (request.isForce()) {
                        forceDeleteJob(request, (JobStorageDeletionTask) task, listener);
                    } else {
                        normalDeleteJob(request, (JobStorageDeletionTask) task, listener);
                    }
                },
                e -> {
                    if (e instanceof MlMetadata.JobAlreadyMarkedAsDeletedException) {
                        // Don't kick off a parallel deletion task, but just wait for
                        // the in-progress request to finish.  This is much safer in the
                        // case where the job with the same name might be immediately
                        // recreated after the delete returns.  However, if a force
                        // delete times out then eventually kick off a parallel delete
                        // in case the original completely failed for some reason.
                        waitForDeletingJob(request.getJobId(), MachineLearningField.STATE_PERSIST_RESTORE_TIMEOUT,
                                ActionListener.wrap(
                                listener::onResponse,
                                e2 -> {
                                    if (request.isForce() && e2 instanceof TimeoutException) {
                                        forceDeleteJob(request, (JobStorageDeletionTask) task, listener);
                                    } else {
                                        listener.onFailure(e2);
                                    }
                                }
                        ));
                    } else {
                        listener.onFailure(e);
                    }
                });

        markJobAsDeleting(request.getJobId(), markAsDeletingListener, request.isForce());
    }

    @Override
    protected void masterOperation(DeleteJobAction.Request request, ClusterState state,
                                   ActionListener<DeleteJobAction.Response> listener) throws Exception {
        throw new UnsupportedOperationException("the Task parameter is required");
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void normalDeleteJob(DeleteJobAction.Request request, JobStorageDeletionTask task,
                                 ActionListener<DeleteJobAction.Response> listener) {
        jobManager.deleteJob(request, task, listener);
    }

    private void forceDeleteJob(DeleteJobAction.Request request, JobStorageDeletionTask task,
                                ActionListener<DeleteJobAction.Response> listener) {

        final ClusterState state = clusterService.state();
        final String jobId = request.getJobId();

        // 3. Delete the job
        ActionListener<Boolean> removeTaskListener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean response) {
                jobManager.deleteJob(request, task, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResourceNotFoundException) {
                    jobManager.deleteJob(request, task, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        };

        // 2. Cancel the persistent task. This closes the process gracefully so
        // the process should be killed first.
        ActionListener<KillProcessAction.Response> killJobListener = ActionListener.wrap(
                response -> {
                    removePersistentTask(request.getJobId(), state, removeTaskListener);
                },
                e -> {
                    if (e instanceof ElasticsearchStatusException) {
                        // Killing the process marks the task as completed so it
                        // may have disappeared when we get here
                        removePersistentTask(request.getJobId(), state, removeTaskListener);
                    } else {
                        listener.onFailure(e);
                    }
                }
        );

        // 1. Kill the job's process
        killProcess(jobId, killJobListener);
    }

    private void killProcess(String jobId, ActionListener<KillProcessAction.Response> listener) {
        KillProcessAction.Request killRequest = new KillProcessAction.Request(jobId);
        executeAsyncWithOrigin(client, ML_ORIGIN, KillProcessAction.INSTANCE, killRequest, listener);
    }

    private void removePersistentTask(String jobId, ClusterState currentState,
                                      ActionListener<Boolean> listener) {
        PersistentTasksCustomMetaData tasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlMetadata.getJobTask(jobId, tasks);
        if (jobTask == null) {
            listener.onResponse(null);
        } else {
            persistentTasksService.cancelPersistentTask(jobTask.getId(),
                    new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                        @Override
                        public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> task) {
                            listener.onResponse(Boolean.TRUE);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
        }
    }

    void markJobAsDeleting(String jobId, ActionListener<Boolean> listener, boolean force) {
        clusterService.submitStateUpdateTask("mark-job-as-deleted", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                PersistentTasksCustomMetaData tasks = currentState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
                MlMetadata.Builder builder = new MlMetadata.Builder(MlMetadata.getMlMetadata(currentState));
                builder.markJobAsDeleted(jobId, tasks, force);
                return buildNewClusterState(currentState, builder);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                logger.debug("Job [" + jobId + "] is successfully marked as deleted");
                listener.onResponse(true);
            }
        });
    }

    void waitForDeletingJob(String jobId, TimeValue timeout, ActionListener<DeleteJobAction.Response> listener) {
        ClusterStateObserver stateObserver = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());

        ClusterState clusterState = stateObserver.setAndGetObservedState();
        if (jobIsDeletedFromState(jobId, clusterState)) {
            listener.onResponse(new DeleteJobAction.Response(true));
        } else {
            stateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(new DeleteJobAction.Response(true));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new TimeoutException("timed out after " + timeout));
                }
            }, newClusterState -> jobIsDeletedFromState(jobId, newClusterState), timeout);
        }
    }

    static boolean jobIsDeletedFromState(String jobId, ClusterState clusterState) {
        return !MlMetadata.getMlMetadata(clusterState).getJobs().containsKey(jobId);
    }

    private static ClusterState buildNewClusterState(ClusterState currentState, MlMetadata.Builder builder) {
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MLMetadataField.TYPE, builder.build()).build());
        return newState.build();
    }
}
