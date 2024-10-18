/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Request;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Response;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.InferenceWaitForAllocation;
import org.elasticsearch.xpack.ml.inference.adaptiveallocations.AdaptiveAllocationsScalerService;
import org.elasticsearch.xpack.ml.inference.adaptiveallocations.ScaleFromZeroFeatureFlag;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportInternalInferModelAction extends HandledTransportAction<Request, Response> {

    private final ModelLoadingService modelLoadingService;
    private final Client client;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;
    private final TrainedModelProvider trainedModelProvider;
    private final AdaptiveAllocationsScalerService adaptiveAllocationsScalerService;
    private final InferenceWaitForAllocation waitForAllocation;
    private final ThreadPool threadPool;

    TransportInternalInferModelAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ModelLoadingService modelLoadingService,
        Client client,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TrainedModelProvider trainedModelProvider,
        AdaptiveAllocationsScalerService adaptiveAllocationsScalerService,
        TrainedModelAssignmentService assignmentService,
        ThreadPool threadPool
    ) {
        super(actionName, transportService, actionFilters, InferModelAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.modelLoadingService = modelLoadingService;
        this.client = client;
        this.clusterService = clusterService;
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.adaptiveAllocationsScalerService = adaptiveAllocationsScalerService;
        this.waitForAllocation = new InferenceWaitForAllocation(assignmentService, this::inferOnBlockedRequest);
        this.threadPool = threadPool;
    }

    @Inject
    public TransportInternalInferModelAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelLoadingService modelLoadingService,
        Client client,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TrainedModelProvider trainedModelProvider,
        AdaptiveAllocationsScalerService adaptiveAllocationsScalerService,
        TrainedModelAssignmentService assignmentService,
        ThreadPool threadPool
    ) {
        this(
            InferModelAction.NAME,
            transportService,
            actionFilters,
            modelLoadingService,
            client,
            clusterService,
            licenseState,
            trainedModelProvider,
            adaptiveAllocationsScalerService,
            assignmentService,
            threadPool
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        Response.Builder responseBuilder = Response.builder();
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());

        if (MachineLearning.INFERENCE_AGG_FEATURE.check(licenseState)) {
            responseBuilder.setLicensed(true);
            doInfer(task, request, responseBuilder, parentTaskId, listener);
        } else {
            trainedModelProvider.getTrainedModel(
                request.getId(),
                GetTrainedModelsAction.Includes.empty(),
                parentTaskId,
                ActionListener.wrap(trainedModelConfig -> {
                    // Since we just checked MachineLearningField.ML_API_FEATURE.check(licenseState) and that check failed
                    // That means we don't have a plat+ license. The only licenses for trained models are basic (free) and plat.
                    boolean allowed = trainedModelConfig.getLicenseLevel() == License.OperationMode.BASIC;
                    responseBuilder.setLicensed(allowed);
                    if (allowed || request.isPreviouslyLicensed()) {
                        doInfer(task, request, responseBuilder, parentTaskId, listener);
                    } else {
                        listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
                    }
                }, listener::onFailure)
            );
        }
    }

    private void doInfer(
        Task task,
        Request request,
        Response.Builder responseBuilder,
        TaskId parentTaskId,
        ActionListener<Response> listener
    ) {
        String concreteModelId = Optional.ofNullable(ModelAliasMetadata.fromState(clusterService.state()).getModelId(request.getId()))
            .orElse(request.getId());

        responseBuilder.setId(concreteModelId);

        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(clusterService.state());
        TrainedModelAssignment assignment = trainedModelAssignmentMetadata.getDeploymentAssignment(concreteModelId);
        List<TrainedModelAssignment> assignments;
        if (assignment == null) {
            // look up by model
            assignments = trainedModelAssignmentMetadata.getDeploymentsUsingModel(concreteModelId);
        } else {
            assignments = List.of(assignment);
        }

        if (assignments.isEmpty()) {
            getModelAndInfer(request, responseBuilder, parentTaskId, (CancellableTask) task, listener);
        } else {
            inferAgainstAllocatedModel(assignments, request, responseBuilder, parentTaskId, listener);
        }
    }

    private void getModelAndInfer(
        Request request,
        Response.Builder responseBuilder,
        TaskId parentTaskId,
        CancellableTask task,
        ActionListener<Response> listener
    ) {
        ActionListener<LocalModel> getModelListener = ActionListener.wrap(model -> {
            TypedChainTaskExecutor<InferenceResults> typedChainTaskExecutor = new TypedChainTaskExecutor<>(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                // run through all tasks
                Predicates.always(),
                // Always fail immediately and return an error
                Predicates.always()
            );
            request.getObjectsToInfer().forEach(stringObjectMap -> typedChainTaskExecutor.add(chainedTask -> {
                if (task.isCancelled()) {
                    throw new TaskCancelledException(format("Inference task cancelled with reason [%s]", task.getReasonCancelled()));
                }
                model.infer(stringObjectMap, request.getUpdate(), chainedTask);
            }));

            typedChainTaskExecutor.execute(ActionListener.wrap(inferenceResultsInterfaces -> {
                model.release();
                listener.onResponse(responseBuilder.addInferenceResults(inferenceResultsInterfaces).build());
            }, e -> {
                model.release();
                listener.onFailure(e);
            }));
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                listener.onFailure(e);
                return;
            }

            // The model was found, check if a more relevant error message can be returned
            trainedModelProvider.getTrainedModel(
                request.getId(),
                GetTrainedModelsAction.Includes.empty(),
                parentTaskId,
                ActionListener.wrap(trainedModelConfig -> {
                    if (trainedModelConfig.getModelType() == TrainedModelType.PYTORCH) {
                        // The PyTorch model cannot be allocated if we got here
                        listener.onFailure(
                            ExceptionsHelper.conflictStatusException(
                                "Model ["
                                    + request.getId()
                                    + "] must be deployed to use. Please deploy with the start trained model deployment API.",
                                request.getId()
                            )
                        );
                    } else {
                        // return the original error
                        listener.onFailure(e);
                    }
                }, listener::onFailure)
            );
        });

        // TODO should `getModelForInternalInference` be used here??
        modelLoadingService.getModelForPipeline(request.getId(), parentTaskId, getModelListener);
    }

    private void inferAgainstAllocatedModel(
        List<TrainedModelAssignment> assignments,
        Request request,
        Response.Builder responseBuilder,
        TaskId parentTaskId,
        ActionListener<Response> listener
    ) {
        TrainedModelAssignment assignment = pickAssignment(assignments);

        if (assignment.getAssignmentState() == AssignmentState.STOPPING || assignment.getAssignmentState() == AssignmentState.FAILED) {
            String message = "Trained model [" + assignment.getDeploymentId() + "] is [" + assignment.getAssignmentState() + "]";
            listener.onFailure(ExceptionsHelper.conflictStatusException(message));
            return;
        }

        // Get a list of nodes to send the requests to and the number of
        // documents for each node.
        var nodes = assignment.selectRandomNodesWeighedOnAllocations(request.numberOfDocuments(), RoutingState.STARTED);

        // We couldn't find any nodes in the started state so let's look for ones that are stopping in case we're shutting down some nodes
        if (nodes.isEmpty()) {
            nodes = assignment.selectRandomNodesWeighedOnAllocations(request.numberOfDocuments(), RoutingState.STOPPING);
        }

        if (nodes.isEmpty()) {
            String message = "Trained model deployment [" + request.getId() + "] is not allocated to any nodes";
            boolean starting = adaptiveAllocationsScalerService.maybeStartAllocation(assignment);
            if (starting) {
                message += "; starting deployment of one allocation";

                if (ScaleFromZeroFeatureFlag.isEnabled()) {
                    waitForAllocation.waitForAssignment(
                        new InferenceWaitForAllocation.WaitingRequest(request, responseBuilder, parentTaskId, listener)
                    );
                    return;
                }
            }

            logger.debug(message);
            listener.onFailure(ExceptionsHelper.conflictStatusException(message));
            return;
        }

        assert nodes.stream().mapToInt(Tuple::v2).sum() == request.numberOfDocuments()
            : "mismatch; sum of node requests does not match number of documents in request";
        inferOnAssignmentNodes(assignment.getDeploymentId(), nodes, request, responseBuilder, parentTaskId, listener);
    }

    private void inferOnBlockedRequest(InferenceWaitForAllocation.WaitingRequest request, TrainedModelAssignment assignment) {
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {

            var nodes = assignment.selectRandomNodesWeighedOnAllocations(request.request().numberOfDocuments(), RoutingState.STARTED);

            if (nodes.isEmpty()) {
                request.listener()
                    .onFailure(
                        new IllegalStateException(
                            "[" + request.deploymentId() + "] error waiting for started allocations. The assignment has 0 started nodes"
                        )
                    );
            }

            inferOnAssignmentNodes(
                assignment.getDeploymentId(),
                nodes,
                request.request(),
                request.responseBuilder(),
                request.parentTaskId(),
                request.listener()
            );
        });
    }

    private void inferOnAssignmentNodes(
        String deploymentId,
        List<Tuple<String, Integer>> nodes,
        Request request,
        Response.Builder responseBuilder,
        TaskId parentTaskId,
        ActionListener<Response> listener
    ) {
        AtomicInteger count = new AtomicInteger();
        AtomicArray<List<InferenceResults>> results = new AtomicArray<>(nodes.size());
        AtomicReference<Exception> failure = new AtomicReference<>();

        int startPos = 0;
        int slot = 0;
        for (var node : nodes) {
            InferTrainedModelDeploymentAction.Request deploymentRequest;
            if (request.getTextInput() == null) {
                deploymentRequest = InferTrainedModelDeploymentAction.Request.forDocs(
                    deploymentId,
                    request.getUpdate(),
                    request.getObjectsToInfer().subList(startPos, startPos + node.v2()),
                    request.getInferenceTimeout()
                );
            } else {
                deploymentRequest = InferTrainedModelDeploymentAction.Request.forTextInput(
                    deploymentId,
                    request.getUpdate(),
                    request.getTextInput().subList(startPos, startPos + node.v2()),
                    request.getInferenceTimeout()
                );
            }
            deploymentRequest.setHighPriority(request.isHighPriority());
            deploymentRequest.setPrefixType(request.getPrefixType());
            deploymentRequest.setNodes(node.v1());
            deploymentRequest.setParentTask(parentTaskId);
            deploymentRequest.setChunkResults(request.isChunked());

            startPos += node.v2();

            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                InferTrainedModelDeploymentAction.INSTANCE,
                deploymentRequest,
                collectingListener(count, results, failure, slot, nodes.size(), responseBuilder, listener)
            );

            slot++;
        }
    }

    static TrainedModelAssignment pickAssignment(List<TrainedModelAssignment> assignments) {
        assert assignments.isEmpty() == false;

        if (assignments.size() == 1) {
            return assignments.get(0);
        }

        var map = assignments.stream().collect(Collectors.groupingBy(TrainedModelAssignment::getAssignmentState));

        Random rng = Randomness.get();
        for (var assignmentStat : new AssignmentState[] {
            AssignmentState.STARTED,
            AssignmentState.STARTING,
            AssignmentState.STOPPING,
            AssignmentState.FAILED }) {
            List<TrainedModelAssignment> bestPick = map.get(assignmentStat);
            if (bestPick != null) {
                Collections.shuffle(bestPick, rng);
                return bestPick.get(0);
            }
        }

        // should never hit this
        throw new IllegalStateException();
    }

    private static ActionListener<InferTrainedModelDeploymentAction.Response> collectingListener(
        AtomicInteger count,
        AtomicArray<List<InferenceResults>> results,
        AtomicReference<Exception> failure,
        int slot,
        int totalNumberOfResponses,
        Response.Builder responseBuilder,
        ActionListener<Response> finalListener
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(InferTrainedModelDeploymentAction.Response response) {
                results.setOnce(slot, response.getResults());
                if (count.incrementAndGet() == totalNumberOfResponses) {
                    sendResponse();
                }
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                if (count.incrementAndGet() == totalNumberOfResponses) {
                    sendResponse();
                }
            }

            private void sendResponse() {
                if (failure.get() != null) {
                    finalListener.onFailure(failure.get());
                } else {
                    for (int i = 0; i < results.length(); i++) {
                        var resultList = results.get(i);
                        if (resultList == null) {
                            continue;
                        }

                        for (var result : resultList) {
                            if (result instanceof ErrorInferenceResults errorResult) {
                                // Any failure fails all requests
                                // TODO is this the correct behaviour for batched requests?
                                finalListener.onFailure(errorResult.getException());
                                return;
                            }
                        }
                        responseBuilder.addInferenceResults(resultList);
                    }
                    finalListener.onResponse(responseBuilder.build());
                }
            }
        };
    }
}
