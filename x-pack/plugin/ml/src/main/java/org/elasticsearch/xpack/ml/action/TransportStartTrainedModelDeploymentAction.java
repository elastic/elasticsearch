/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.common.time.RemainingTime;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.TransportVersionUtils;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.assignment.ModelDeploymentTimeoutException;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.TaskRetriever;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction.MAX_NUM_NATIVE_DEFINITION_PARTS;

public class TransportStartTrainedModelDeploymentAction extends TransportMasterNodeAction<
    StartTrainedModelDeploymentAction.Request,
    CreateTrainedModelAssignmentAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStartTrainedModelDeploymentAction.class);

    private final XPackLicenseState licenseState;
    private final OriginSettingClient client;
    private final TrainedModelAssignmentService trainedModelAssignmentService;
    private final MlMemoryTracker memoryTracker;
    private final InferenceAuditor auditor;

    @Inject
    public TransportStartTrainedModelDeploymentAction(
        TransportService transportService,
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        TrainedModelAssignmentService trainedModelAssignmentService,
        MlMemoryTracker memoryTracker,
        InferenceAuditor auditor
    ) {
        super(
            StartTrainedModelDeploymentAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            StartTrainedModelDeploymentAction.Request::new,
            CreateTrainedModelAssignmentAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseState = Objects.requireNonNull(licenseState);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ML_ORIGIN);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        this.trainedModelAssignmentService = Objects.requireNonNull(trainedModelAssignmentService);
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected void masterOperation(
        Task task,
        StartTrainedModelDeploymentAction.Request request,
        ClusterState state,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) throws Exception {
        var remainingTime = RemainingTime.from(Instant::now, request.getTimeout());
        logger.debug(() -> "[" + request.getDeploymentId() + "] received deploy request for model [" + request.getModelId() + "]");
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        if (TransportVersionUtils.isMinTransportVersionSameAsCurrent(state) == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot start model deployment [{}] while cluster upgrade is in progress.",
                    RestStatus.FORBIDDEN,
                    request.getDeploymentId()
                )
            );
            return;
        }

        var assignments = TrainedModelAssignmentMetadata.fromState(state);
        if (assignments.allAssignments().size() >= MachineLearning.MAX_TRAINED_MODEL_DEPLOYMENTS) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Could not start model deployment because existing deployments reached the limit of [{}]",
                    RestStatus.TOO_MANY_REQUESTS,
                    MachineLearning.MAX_TRAINED_MODEL_DEPLOYMENTS
                )
            );
            return;
        }

        if (assignments.getDeploymentAssignment(request.getDeploymentId()) != null) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Could not start model deployment because an existing deployment with the same id [{}] exist",
                    RestStatus.BAD_REQUEST,
                    request.getDeploymentId()
                )
            );
            return;
        }

        AtomicLong perDeploymentMemoryBytes = new AtomicLong();
        AtomicLong perAllocationMemoryBytes = new AtomicLong();

        ActionListener<CreateTrainedModelAssignmentAction.Response> waitForDeploymentToStart = ActionListener.wrap(
            modelAssignment -> waitForDeploymentState(request, remainingTime.get(), listener),
            e -> {
                logger.warn(
                    () -> "[" + request.getDeploymentId() + "] creating new assignment for model [" + request.getModelId() + "] failed",
                    e
                );
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException resourceAlreadyExistsException) {
                    e = new ElasticsearchStatusException(
                        "Cannot start deployment [{}] because it has already been started",
                        RestStatus.CONFLICT,
                        resourceAlreadyExistsException,
                        request.getDeploymentId()
                    );
                }
                listener.onFailure(e);
            }
        );

        ActionListener<Tuple<String, Long>> modelSizeListener = ActionListener.wrap(modelIdAndSizeInBytes -> {
            TaskParams taskParams = new TaskParams(
                modelIdAndSizeInBytes.v1(),
                request.getDeploymentId(),
                modelIdAndSizeInBytes.v2(),
                request.computeNumberOfAllocations(),
                request.getThreadsPerAllocation(),
                request.getQueueCapacity(),
                Optional.ofNullable(request.getCacheSize()).orElse(ByteSizeValue.ofBytes(modelIdAndSizeInBytes.v2())),
                request.getPriority(),
                perDeploymentMemoryBytes.get(),
                perAllocationMemoryBytes.get()
            );
            PersistentTasksCustomMetadata persistentTasks = clusterService.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            memoryTracker.refresh(
                persistentTasks,
                ActionListener.wrap(
                    aVoid -> trainedModelAssignmentService.createNewModelAssignment(
                        new CreateTrainedModelAssignmentAction.Request(taskParams, request.getAdaptiveAllocationsSettings()),
                        waitForDeploymentToStart
                    ),
                    listener::onFailure
                )
            );
        }, listener::onFailure);

        GetTrainedModelsAction.Request getModelWithDeploymentId = new GetTrainedModelsAction.Request(request.getDeploymentId());

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(getModelResponse -> {
            if (getModelResponse.getResources().results().size() > 1) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "cannot deploy more than one model at the same time; [{}] matches models [{}]",
                        request.getModelId(),
                        getModelResponse.getResources().results().stream().map(TrainedModelConfig::getModelId).toList()
                    )
                );
                return;
            }

            TrainedModelConfig trainedModelConfig = getModelResponse.getResources().results().get(0);
            if (trainedModelConfig.getModelType() != TrainedModelType.PYTORCH) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "model [{}] of type [{}] cannot be deployed. Only PyTorch models can be deployed",
                        trainedModelConfig.getModelId(),
                        trainedModelConfig.getModelType()
                    )
                );
                return;
            }

            perDeploymentMemoryBytes.set(trainedModelConfig.getPerDeploymentMemoryBytes());
            perAllocationMemoryBytes.set(trainedModelConfig.getPerAllocationMemoryBytes());

            if (trainedModelConfig.getLocation() == null) {
                listener.onFailure(ExceptionsHelper.serverError("model [{}] does not have location", trainedModelConfig.getModelId()));
                return;
            }

            ActionListener<GetTrainedModelsAction.Response> checkDeploymentIdDoesntAlreadyExist = ActionListener.wrap(
                response -> listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Deployment id [{}] is the same as an another model which is not the model being deployed. "
                            + "Deployment id can be the same as the model being deployed but cannot match a different model",
                        request.getDeploymentId(),
                        request.getModelId()
                    )
                ),
                error -> {
                    if (ExceptionsHelper.unwrapCause(error) instanceof ResourceNotFoundException) {
                        // no name clash, continue with the deployment
                        checkFullModelDefinitionIsPresent(client, trainedModelConfig, true, remainingTime.get(), modelSizeListener);
                    } else {
                        listener.onFailure(error);
                    }
                }
            );

            // If the model id isn't the same id as the deployment id
            // check there isn't another model with that deployment id
            if (request.getModelId().equals(request.getDeploymentId()) == false) {
                client.execute(GetTrainedModelsAction.INSTANCE, getModelWithDeploymentId, checkDeploymentIdDoesntAlreadyExist);
            } else {
                checkFullModelDefinitionIsPresent(client, trainedModelConfig, true, remainingTime.get(), modelSizeListener);
            }

        }, listener::onFailure);

        ActionListener<GetInferenceModelAction.Response> getInferenceModelListener = ActionListener.wrap((getInferenceModelResponse) -> {
            if (getInferenceModelResponse.getEndpoints().isEmpty() == false) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(Messages.MODEL_ID_MATCHES_EXISTING_MODEL_IDS_BUT_MUST_NOT, request.getModelId())
                );
            } else {
                getTrainedModelRequestExecution(request, getModelListener);
            }
        }, error -> {
            if (ExceptionsHelper.unwrapCause(error) instanceof ResourceNotFoundException) {
                // no name clash, continue with the deployment
                getTrainedModelRequestExecution(request, getModelListener);
            } else {
                listener.onFailure(error);
            }
        });

        GetInferenceModelAction.Request getModelRequest = new GetInferenceModelAction.Request(request.getModelId(), TaskType.ANY);
        client.execute(GetInferenceModelAction.INSTANCE, getModelRequest, getInferenceModelListener);
    }

    private void getTrainedModelRequestExecution(
        StartTrainedModelDeploymentAction.Request request,
        ActionListener<GetTrainedModelsAction.Response> getModelListener
    ) {
        GetTrainedModelsAction.Request getModelRequest = new GetTrainedModelsAction.Request(request.getModelId());
        client.execute(GetTrainedModelsAction.INSTANCE, getModelRequest, getModelListener);
    }

    private void waitForDeploymentState(
        StartTrainedModelDeploymentAction.Request request,
        TimeValue remainingTime,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) {
        var deploymentId = request.getDeploymentId();
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate(deploymentId, request.getWaitForState());
        trainedModelAssignmentService.waitForAssignmentCondition(
            deploymentId,
            predicate,
            remainingTime,
            new TrainedModelAssignmentService.WaitForAssignmentListener() {
                @Override
                public void onResponse(TrainedModelAssignment assignment) {
                    if (predicate.exception != null) {
                        deleteFailedDeployment(deploymentId, predicate.exception, listener);
                    } else {
                        auditor.info(assignment.getDeploymentId(), Messages.INFERENCE_DEPLOYMENT_STARTED);
                        listener.onResponse(new CreateTrainedModelAssignmentAction.Response(assignment));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    onFailure(
                        new ModelDeploymentTimeoutException(
                            format(
                                "Timed out after [%s] waiting for trained model deployment [%s] to start. "
                                    + "Use the trained model stats API to track the state of the deployment "
                                    + "and try again once it has started.",
                                request.getTimeout(),
                                request.getDeploymentId()
                            )
                        )
                    );
                }
            }
        );
    }

    private void deleteFailedDeployment(
        String deploymentId,
        Exception exception,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) {
        logger.trace(() -> format("[%s] Deleting failed deployment", deploymentId), exception);
        trainedModelAssignmentService.deleteModelAssignment(deploymentId, ActionListener.wrap(pTask -> listener.onFailure(exception), e -> {
            logger.error(
                () -> format(
                    "[%s] Failed to delete model allocation that had failed with the reason [%s]",
                    deploymentId,
                    exception.getMessage()
                ),
                e
            );
            listener.onFailure(exception);
        }));

    }

    /**
     * The model definition is spread over multiple docs.
     * Check that all docs are present by summing up the
     * individual per-document definition lengths and checking
     * the total is equal to the total definition length as
     * stored in the docs.
     * <p>
     * On success the response is a tuple
     * {@code <String, Long> (model id, total definition length)}
     * <p>
     * If {@code errorIfDefinitionIsMissing == false} and some
     * definition docs are missing then {@code listener::onResponse}
     * is called with the total definition length == 0.
     * This usage is to answer yes/no questions if the full model
     * definition is present.
     *
     * @param mlOriginClient             A client using ML_ORIGIN
     * @param config                     trained model config
     * @param errorIfDefinitionIsMissing If true missing definition parts cause errors.
     *                                   If false and some parts are missing the total
     *                                   definition length in the response is set to 0.
     * @param timeout                    The timeout value in seconds that the request should fail if it does not complete.
     *                                   If null, then this will not wait for the download to complete before returning.
     * @param listener                   response listener
     */
    static void checkFullModelDefinitionIsPresent(
        OriginSettingClient mlOriginClient,
        TrainedModelConfig config,
        boolean errorIfDefinitionIsMissing,
        TimeValue timeout,
        ActionListener<Tuple<String, Long>> listener
    ) {
        if (config.getLocation() instanceof IndexLocation == false) {
            listener.onResponse(null);
            return;
        }

        final String modelId = config.getModelId();

        String index = ((IndexLocation) config.getLocation()).getIndexName();

        // Step 3
        // Verify the model definition parts are all present
        ActionListener<SearchResponse> step3SearchResultsVerificationListener = step3VerifyModelPartsArePresent(
            errorIfDefinitionIsMissing,
            listener,
            modelId
        );

        // Step 2
        // Search for the model definition parts to ensure they are all present
        ActionListener<Runnable> step2DocsSearchListener = step2SearchForModelParts(
            mlOriginClient,
            listener,
            index,
            modelId,
            step3SearchResultsVerificationListener
        );

        // Step 1 (there is no step zero)
        // Check if there is a download task for this model, and wait for it to complete or timeout
        step1CheckForDownloadTask(mlOriginClient, errorIfDefinitionIsMissing, timeout, listener, modelId, step2DocsSearchListener);
    }

    private static ActionListener<SearchResponse> step3VerifyModelPartsArePresent(
        boolean errorIfDefinitionIsMissing,
        ActionListener<Tuple<String, Long>> listener,
        String modelId
    ) {
        return ActionListener.wrap(response -> {
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length == 0) {
                failOrRespondWith0(
                    () -> new ResourceNotFoundException(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)),
                    errorIfDefinitionIsMissing,
                    modelId,
                    listener
                );
                return;
            }

            long firstTotalLength;
            DocumentField firstTotalLengthField = hits[0].field(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName());
            if (firstTotalLengthField != null && firstTotalLengthField.getValue() instanceof Long firstTotalDefinitionLength) {
                firstTotalLength = firstTotalDefinitionLength;
            } else {
                failOrRespondWith0(
                    () -> missingFieldsError(
                        modelId,
                        hits[0].getId(),
                        List.of(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName())
                    ),
                    errorIfDefinitionIsMissing,
                    modelId,
                    listener
                );
                return;
            }

            Set<String> missingFields = new HashSet<>();
            long summedLengths = 0;
            for (SearchHit hit : hits) {
                long totalLength = -1;
                DocumentField totalLengthField = hit.field(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName());
                if (totalLengthField != null && totalLengthField.getValue() instanceof Long totalDefinitionLength) {
                    totalLength = totalDefinitionLength;
                } else {
                    missingFields.add(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName());
                }

                DocumentField definitionLengthField = hit.field(TrainedModelDefinitionDoc.DEFINITION_LENGTH.getPreferredName());
                if (definitionLengthField != null && definitionLengthField.getValue() instanceof Long definitionLength) {
                    summedLengths += definitionLength;
                } else {
                    missingFields.add(TrainedModelDefinitionDoc.DEFINITION_LENGTH.getPreferredName());
                }

                if (missingFields.isEmpty() == false) {
                    failOrRespondWith0(
                        () -> missingFieldsError(modelId, hit.getId(), missingFields),
                        errorIfDefinitionIsMissing,
                        modelId,
                        listener
                    );
                    return;
                }

                if (totalLength != firstTotalLength) {
                    final long finalTotalLength = totalLength;
                    failOrRespondWith0(
                        () -> ExceptionsHelper.badRequestException(
                            "[{}] [total_definition_length] must be the same in all model definition parts. "
                                + "The value [{}] in model definition part [{}] does not match the value [{}] in part [{}]. "
                                + Messages.UNABLE_TO_DEPLOY_MODEL_BAD_PARTS,
                            modelId,
                            finalTotalLength,
                            TrainedModelDefinitionDoc.docNum(modelId, Objects.requireNonNull(hit.getId())),
                            firstTotalLength,
                            TrainedModelDefinitionDoc.docNum(modelId, Objects.requireNonNull(hits[0].getId()))
                        ),
                        errorIfDefinitionIsMissing,
                        modelId,
                        listener
                    );
                    return;
                }

            }
            if (summedLengths != firstTotalLength) {
                failOrRespondWith0(
                    () -> ExceptionsHelper.badRequestException(Messages.getMessage(Messages.MODEL_DEFINITION_TRUNCATED, modelId)),
                    errorIfDefinitionIsMissing,
                    modelId,
                    listener
                );
                return;
            }
            listener.onResponse(new Tuple<>(modelId, summedLengths));
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                failOrRespondWith0(() -> {
                    Exception ex = new ResourceNotFoundException(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId));
                    ex.addSuppressed(e);
                    return ex;
                }, errorIfDefinitionIsMissing, modelId, listener);
            } else {
                listener.onFailure(e);
            }
        });
    }

    private static ActionListener<Runnable> step2SearchForModelParts(
        OriginSettingClient mlOriginClient,
        ActionListener<Tuple<String, Long>> listener,
        String index,
        String modelId,
        ActionListener<SearchResponse> nextStepListener
    ) {
        return ActionListener.wrap(r -> {
            mlOriginClient.prepareSearch(index)
                .setQuery(
                    QueryBuilders.constantScoreQuery(
                        QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId))
                            .filter(
                                QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(), TrainedModelDefinitionDoc.NAME)
                            )
                    )
                )
                .setFetchSource(false)
                .addDocValueField(TrainedModelDefinitionDoc.DEFINITION_LENGTH.getPreferredName())
                .addDocValueField(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName())
                .setSize(MAX_NUM_NATIVE_DEFINITION_PARTS)
                .setTrackTotalHits(true)
                .addSort(
                    SortBuilders.fieldSort(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName()).order(SortOrder.ASC).unmappedType("long")
                )
                .execute(nextStepListener);

        }, listener::onFailure);
    }

    /*
    @param timeout null value indicates that the request should not wait for the download to complete before returning
     */
    private static void step1CheckForDownloadTask(
        OriginSettingClient mlOriginClient,
        boolean errorIfDefinitionIsMissing,
        TimeValue timeout,
        ActionListener<Tuple<String, Long>> failureListener,
        String modelId,
        ActionListener<Runnable> nextStepListener
    ) {
        // check task is present, do not wait for completion
        TaskRetriever.getDownloadTaskInfo(
            mlOriginClient,
            modelId,
            timeout != null,
            timeout,
            () -> Messages.getMessage(Messages.MODEL_DOWNLOAD_IN_PROGRESS, modelId),
            ActionListener.wrap(taskInfo -> {
                if (taskInfo == null) {
                    nextStepListener.onResponse(null);
                } else {
                    failOrRespondWith0(
                        () -> new ElasticsearchStatusException(
                            Messages.getMessage(Messages.MODEL_DOWNLOAD_IN_PROGRESS, modelId),
                            RestStatus.REQUEST_TIMEOUT
                        ),
                        errorIfDefinitionIsMissing,
                        modelId,
                        failureListener
                    );
                }
            }, failureListener::onFailure)
        );
    }

    private static void failOrRespondWith0(
        Supplier<Exception> exceptionSupplier,
        boolean errorIfDefinitionIsMissing,
        String modelId,
        ActionListener<Tuple<String, Long>> listener
    ) {
        if (errorIfDefinitionIsMissing) {
            listener.onFailure(exceptionSupplier.get());
        } else {
            listener.onResponse(new Tuple<>(modelId, 0L));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(StartTrainedModelDeploymentAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private static ElasticsearchStatusException missingFieldsError(String modelId, String hitId, Collection<String> missingFields) {
        return ExceptionsHelper.badRequestException(
            "[{}] model definition [{}] is missing required fields {}. {}",
            modelId,
            hitId,
            missingFields,
            Messages.UNABLE_TO_DEPLOY_MODEL_BAD_PARTS
        );
    }

    private static class DeploymentStartedPredicate implements Predicate<ClusterState> {

        private volatile Exception exception;

        // for logging
        private final String deploymentId;
        private final AllocationStatus.State waitForState;

        DeploymentStartedPredicate(String deploymentId, AllocationStatus.State waitForState) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, "deployment_id");
            this.waitForState = waitForState;
        }

        @Override
        public boolean test(ClusterState clusterState) {
            TrainedModelAssignment trainedModelAssignment = TrainedModelAssignmentMetadata.assignmentForDeploymentId(
                clusterState,
                deploymentId
            ).orElse(null);
            if (trainedModelAssignment == null) {
                // The assignment may be null if it was stopped by another action while waiting
                this.exception = new ElasticsearchStatusException(
                    "Error waiting for the model deployment to start. The trained model assignment was removed while waiting",
                    RestStatus.BAD_REQUEST
                );
                logger.trace(() -> format("[%s] assignment was null while waiting for state [%s]", deploymentId, waitForState));
                return true;
            }

            final Set<Map.Entry<String, RoutingInfo>> nodeIdsAndRouting = trainedModelAssignment.getNodeRoutingTable().entrySet();

            Map<String, String> nodeFailuresAndReasons = new HashMap<>();
            Set<String> nodesStillInitializing = new LinkedHashSet<>();
            for (Map.Entry<String, RoutingInfo> nodeIdAndRouting : nodeIdsAndRouting) {
                if (RoutingState.FAILED.equals(nodeIdAndRouting.getValue().getState())) {
                    nodeFailuresAndReasons.put(nodeIdAndRouting.getKey(), nodeIdAndRouting.getValue().getReason());
                }
                if (RoutingState.STARTING.equals(nodeIdAndRouting.getValue().getState())) {
                    nodesStillInitializing.add(nodeIdAndRouting.getKey());
                }
            }

            if (nodeFailuresAndReasons.isEmpty() == false) {
                exception = new ElasticsearchStatusException(
                    "Could not start trained model deployment, the following nodes failed with errors [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    nodeFailuresAndReasons
                );
                return true;
            }
            Set<String> nodesShuttingDown = nodesShuttingDown(clusterState);
            List<DiscoveryNode> nodes = clusterState.nodes()
                .stream()
                .filter(d -> nodesShuttingDown.contains(d.getId()) == false)
                .filter(TaskParams::mayAssignToNode)
                .toList();

            AllocationStatus allocationStatus = trainedModelAssignment.calculateAllocationStatus().orElse(null);
            if (allocationStatus == null || allocationStatus.calculateState().compareTo(waitForState) >= 0) {
                return true;
            }

            if (nodesStillInitializing.isEmpty()) {
                return true;
            }
            logger.trace(
                () -> format(
                    "[%s] tested with state [%s] and nodes %s still initializing",
                    deploymentId,
                    trainedModelAssignment.getAssignmentState(),
                    nodesStillInitializing
                )
            );
            return false;
        }
    }

    static Set<String> nodesShuttingDown(final ClusterState state) {
        return state.metadata().nodeShutdowns().getAllNodeIds();
    }
}
