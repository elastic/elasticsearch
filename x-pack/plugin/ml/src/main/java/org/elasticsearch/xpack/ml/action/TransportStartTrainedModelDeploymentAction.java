/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentState;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentTaskState;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.deployment.DeploymentManager;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.elasticsearch.xpack.ml.inference.persistence.ChunkedTrainedModelRestorer;
import org.elasticsearch.xpack.ml.job.JobNodeSelector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.task.AbstractJobPersistentTasksExecutor;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

public class TransportStartTrainedModelDeploymentAction
    extends TransportMasterNodeAction<StartTrainedModelDeploymentAction.Request, NodeAcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportStartTrainedModelDeploymentAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final PersistentTasksService persistentTasksService;
    private final NamedXContentRegistry xContentRegistry;
    private final MlMemoryTracker memoryTracker;

    @Inject
    public TransportStartTrainedModelDeploymentAction(TransportService transportService, Client client, ClusterService clusterService,
                                                      ThreadPool threadPool, ActionFilters actionFilters, XPackLicenseState licenseState,
                                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                                      PersistentTasksService persistentTasksService,
                                                      NamedXContentRegistry xContentRegistry, MlMemoryTracker memoryTracker) {
        super(StartTrainedModelDeploymentAction.NAME, transportService, clusterService, threadPool, actionFilters,
            StartTrainedModelDeploymentAction.Request::new, indexNameExpressionResolver, NodeAcknowledgedResponse::new,
            ThreadPool.Names.SAME);
        this.licenseState = Objects.requireNonNull(licenseState);
        this.client = Objects.requireNonNull(client);
        this.persistentTasksService = Objects.requireNonNull(persistentTasksService);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
    }

    @Override
    protected void masterOperation(Task task, StartTrainedModelDeploymentAction.Request request, ClusterState state,
                                   ActionListener<NodeAcknowledgedResponse> listener) throws Exception {
        logger.debug(() -> new ParameterizedMessage("[{}] received deploy request", request.getModelId()));
        if (licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        ActionListener<PersistentTasksCustomMetadata.PersistentTask<TaskParams>> waitForDeploymentToStart =
            ActionListener.wrap(
                startedTask -> waitForDeploymentStarted(startedTask, request.getTimeout(), listener),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException(
                            "Cannot start deployment [{}] because it has already been started",
                            RestStatus.CONFLICT,
                            e,
                            request.getModelId()
                        );
                    }
                    listener.onFailure(e);
                }
            );

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(
            getModelResponse -> {
                if (getModelResponse.getResources().results().size() > 1) {
                    listener.onFailure(ExceptionsHelper.badRequestException(
                        "cannot deploy more than one models at the same time; [{}] matches [{}] models]",
                        request.getModelId(), getModelResponse.getResources().results().size()));
                    return;
                }


                TrainedModelConfig trainedModelConfig = getModelResponse.getResources().results().get(0);
                if (trainedModelConfig.getModelType() != TrainedModelType.PYTORCH) {
                    listener.onFailure(ExceptionsHelper.badRequestException(
                        "model [{}] of type [{}] cannot be deployed. Only PyTorch models can be deployed",
                        trainedModelConfig.getModelId(), trainedModelConfig.getModelType()));
                    return;
                }

                if (trainedModelConfig.getLocation() == null) {
                    listener.onFailure(ExceptionsHelper.serverError(
                        "model [{}] does not have location", trainedModelConfig.getModelId()));
                    return;
                }

                getModelBytes(trainedModelConfig, ActionListener.wrap(
                    modelBytes -> {
                        TaskParams taskParams = new TaskParams(
                            trainedModelConfig.getLocation().getModelId(),
                            trainedModelConfig.getLocation().getResourceName(),
                            modelBytes
                        );
                        PersistentTasksCustomMetadata persistentTasks = clusterService.state().getMetadata().custom(
                            PersistentTasksCustomMetadata.TYPE);
                        memoryTracker.refresh(persistentTasks, ActionListener.wrap(
                                aVoid -> persistentTasksService.sendStartRequest(
                                    MlTasks.trainedModelDeploymentTaskId(request.getModelId()),
                                    MlTasks.TRAINED_MODEL_DEPLOYMENT_TASK_NAME,
                                    taskParams,
                                    waitForDeploymentToStart
                                ),
                                listener::onFailure
                            )
                        );
                    },
                    listener::onFailure
                ));
            },
            listener::onFailure
        );

        GetTrainedModelsAction.Request getModelRequest = new GetTrainedModelsAction.Request(request.getModelId());
        client.execute(GetTrainedModelsAction.INSTANCE, getModelRequest, getModelListener);
    }

    private void getModelBytes(TrainedModelConfig trainedModelConfig, ActionListener<Long> listener) {
        ChunkedTrainedModelRestorer restorer = new ChunkedTrainedModelRestorer(trainedModelConfig.getLocation().getModelId(),
            client, threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME), xContentRegistry);
        restorer.setSearchIndex(trainedModelConfig.getLocation().getResourceName());
        restorer.setSearchSize(1);
        restorer.restoreModelDefinition(
            doc -> {
                // The in-memory size of the model was found to be approximately equal
                // to the size of the model on disk in experiments for BERT models. However,
                // this might not always be the case.
                // TODO Improve heuristic for in-memory model size.
                listener.onResponse(doc.getTotalDefinitionLength());

                // Return false to stop the restorer as we only need the first doc
                return false;
            },
            success -> { /* nothing to do */ },
            listener::onFailure
        );
    }

    private void waitForDeploymentStarted(PersistentTasksCustomMetadata.PersistentTask<TaskParams> task,
                                          TimeValue timeout, ActionListener<NodeAcknowledgedResponse> listener) {
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate();
        persistentTasksService.waitForPersistentTaskCondition(task.getId(), predicate, timeout,
            new PersistentTasksService.WaitForPersistentTaskListener<PersistentTaskParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> persistentTask) {
                    if (predicate.exception != null) {
                        cancelFailedDeployment(task, predicate.exception, listener);
                    } else {
                        listener.onResponse(new NodeAcknowledgedResponse(true, predicate.node));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private void cancelFailedDeployment(
        PersistentTasksCustomMetadata.PersistentTask<TaskParams> persistentTask, Exception exception,
        ActionListener<NodeAcknowledgedResponse> listener) {
        persistentTasksService.sendRemoveRequest(persistentTask.getId(), ActionListener.wrap(
            pTask -> listener.onFailure(exception),
            e -> {
                logger.error(
                    new ParameterizedMessage("[{}] Failed to cancel persistent task that had failed with the reason [{}]",
                        persistentTask.getParams().getModelId(), exception.getMessage()),
                    e
                );
                listener.onFailure(exception);
            }
        ));

    }

    @Override
    protected ClusterBlockException checkBlock(StartTrainedModelDeploymentAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private static class DeploymentStartedPredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {

        private volatile Exception exception;
        private volatile String node = "";

        @Override
        public boolean test(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }

            PersistentTasksCustomMetadata.Assignment assignment = persistentTask.getAssignment();

            String reason = "__unknown__";

            if (assignment != null) {
                if (assignment.equals(JobNodeSelector.AWAITING_LAZY_ASSIGNMENT)) {
                    return true;
                }
                if (assignment.equals(PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT) == false && assignment.isAssigned() == false) {
                    exception = new ElasticsearchStatusException("Could not start trained model deployment, allocation explanation [{}]",
                        RestStatus.TOO_MANY_REQUESTS, assignment.getExplanation());
                    return true;
                }
            }

            TrainedModelDeploymentTaskState taskState = (TrainedModelDeploymentTaskState) persistentTask.getState();
            reason = taskState != null ? taskState.getReason() : reason;
            TrainedModelDeploymentState deploymentState = taskState == null ? TrainedModelDeploymentState.STARTING : taskState.getState();

            switch (deploymentState) {
                case STARTED:
                    node = persistentTask.getExecutorNode();
                    return true;
                case STARTING:
                case STOPPING:
                case STOPPED:
                    return false;
                case FAILED:
                    exception = ExceptionsHelper.serverError("Deployment failed with reason: {}", reason);
                    return true;
                default:
                    exception = ExceptionsHelper.serverError("Unexpected task state [{}] with reason [{}] while waiting to be started",
                        taskState.getState(), reason);
                    return true;
            }
        }
    }

    public static class TaskExecutor extends AbstractJobPersistentTasksExecutor<TaskParams> {

        private final DeploymentManager manager;

        public TaskExecutor(Settings settings, ClusterService clusterService, IndexNameExpressionResolver expressionResolver,
                               MlMemoryTracker memoryTracker, DeploymentManager manager) {
            super(MlTasks.TRAINED_MODEL_DEPLOYMENT_TASK_NAME,
                MachineLearning.UTILITY_THREAD_POOL_NAME,
                settings,
                clusterService,
                memoryTracker,
                expressionResolver);
            this.manager = Objects.requireNonNull(manager);
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<TaskParams> persistentTask,
            Map<String, String> headers) {
            return new TrainedModelDeploymentTask(id, type, action, parentTaskId, headers, persistentTask.getParams());
        }

        @Override
        public PersistentTasksCustomMetadata.Assignment getAssignment(TaskParams params,
                                                                      Collection<DiscoveryNode> candidateNodes,
                                                                      ClusterState clusterState) {

            boolean isMemoryTrackerRecentlyRefreshed = memoryTracker.isRecentlyRefreshed();
            Optional<PersistentTasksCustomMetadata.Assignment> optionalAssignment =
                getPotentialAssignment(params, clusterState, isMemoryTrackerRecentlyRefreshed);
            // NOTE: this will return here if isMemoryTrackerRecentlyRefreshed is false, we don't allow assignment with stale memory
            if (optionalAssignment.isPresent()) {
                return optionalAssignment.get();
            }

            JobNodeSelector jobNodeSelector =
                new JobNodeSelector(
                    clusterState,
                    candidateNodes,
                    params.getModelId(),
                    MlTasks.TRAINED_MODEL_DEPLOYMENT_TASK_NAME,
                    memoryTracker,
                    maxLazyMLNodes,
                    node -> nodeFilter(node, params)
                );

            PersistentTasksCustomMetadata.Assignment assignment = jobNodeSelector.selectNode(
                params.estimateMemoryUsageBytes(),
                maxOpenJobs,
                Integer.MAX_VALUE,
                maxMachineMemoryPercent,
                maxNodeMemory,
                useAutoMemoryPercentage
            );
            return assignment;
        }

        public static String nodeFilter(DiscoveryNode node, TaskParams params) {
            String id = params.getModelId();

            if (node.getVersion().before(TaskParams.VERSION_INTRODUCED)) {
                return "Not opening job [" + id + "] on node [" + JobNodeSelector.nodeNameAndVersion(node)
                    + "], because the data frame analytics requires a node of version ["
                    + TaskParams.VERSION_INTRODUCED + "] or higher";
            }

            return null;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TaskParams params, PersistentTaskState state) {
            TrainedModelDeploymentTask trainedModelDeploymentTask = (TrainedModelDeploymentTask) task;
            trainedModelDeploymentTask.setDeploymentManager(manager);

            TrainedModelDeploymentTaskState deployingState = new TrainedModelDeploymentTaskState(
                TrainedModelDeploymentState.STARTING, task.getAllocationId(), null);
            task.updatePersistentTaskState(deployingState, ActionListener.wrap(
                response -> manager.startDeployment(trainedModelDeploymentTask),
                task::markAsFailed
            ));
        }

        @Override
        protected String[] indicesOfInterest(TaskParams params) {
            return new String[] {
                InferenceIndexConstants.INDEX_PATTERN
            };
        }

        @Override
        protected String getJobId(TaskParams params) {
            return params.getModelId();
        }
    }
}
