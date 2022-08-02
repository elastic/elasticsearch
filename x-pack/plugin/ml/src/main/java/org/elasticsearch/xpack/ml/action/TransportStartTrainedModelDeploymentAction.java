/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
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
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackField;
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
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;
import org.elasticsearch.xpack.ml.inference.persistence.ChunkedTrainedModelRestorer;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction.MAX_NUM_NATIVE_DEFINITION_PARTS;

public class TransportStartTrainedModelDeploymentAction extends TransportMasterNodeAction<
    StartTrainedModelDeploymentAction.Request,
    CreateTrainedModelAssignmentAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStartTrainedModelDeploymentAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final TrainedModelAssignmentService trainedModelAssignmentService;
    private final NamedXContentRegistry xContentRegistry;
    private final MlMemoryTracker memoryTracker;
    private final InferenceAuditor auditor;
    protected volatile int maxLazyMLNodes;
    protected volatile long maxMLNodeSize;

    @Inject
    public TransportStartTrainedModelDeploymentAction(
        TransportService transportService,
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        TrainedModelAssignmentService trainedModelAssignmentService,
        NamedXContentRegistry xContentRegistry,
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
            indexNameExpressionResolver,
            CreateTrainedModelAssignmentAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.licenseState = Objects.requireNonNull(licenseState);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ML_ORIGIN);
        this.xContentRegistry = Objects.requireNonNull(xContentRegistry);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        this.trainedModelAssignmentService = Objects.requireNonNull(trainedModelAssignmentService);
        this.auditor = Objects.requireNonNull(auditor);
        this.maxLazyMLNodes = MachineLearning.MAX_LAZY_ML_NODES.get(settings);
        this.maxMLNodeSize = MachineLearning.MAX_ML_NODE_SIZE.get(settings).getBytes();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_LAZY_ML_NODES, this::setMaxLazyMLNodes);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_ML_NODE_SIZE, this::setMaxMLNodeSize);
    }

    private void setMaxLazyMLNodes(int value) {
        this.maxLazyMLNodes = value;
    }

    private void setMaxMLNodeSize(ByteSizeValue value) {
        this.maxMLNodeSize = value.getBytes();
    }

    @Override
    protected void masterOperation(
        Task task,
        StartTrainedModelDeploymentAction.Request request,
        ClusterState state,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) throws Exception {
        logger.trace(() -> "[" + request.getModelId() + "] received deploy request");
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        if (state.nodes().getMaxNodeVersion().after(state.nodes().getMinNodeVersion())) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot start a new model deployment as not all nodes are on version {}. All nodes must be the same version",
                    RestStatus.FORBIDDEN,
                    state.getNodes().getMaxNodeVersion()
                )
            );
            return;
        }

        if (TrainedModelAssignmentMetadata.fromState(state).modelAssignments().size() >= MachineLearning.MAX_TRAINED_MODEL_DEPLOYMENTS) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Could not start model deployment because existing deployments reached the limit of [{}]",
                    RestStatus.TOO_MANY_REQUESTS,
                    MachineLearning.MAX_TRAINED_MODEL_DEPLOYMENTS
                )
            );
            return;
        }

        ActionListener<CreateTrainedModelAssignmentAction.Response> waitForDeploymentToStart = ActionListener.wrap(
            modelAssignment -> waitForDeploymentState(request.getModelId(), request.getTimeout(), request.getWaitForState(), listener),
            e -> {
                logger.warn(() -> "[" + request.getModelId() + "] creating new assignment failed", e);
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

        ActionListener<GetTrainedModelsAction.Response> getModelListener = ActionListener.wrap(getModelResponse -> {
            if (getModelResponse.getResources().results().size() > 1) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "cannot deploy more than one models at the same time; [{}] matches [{}] models]",
                        request.getModelId(),
                        getModelResponse.getResources().results().size()
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

            if (trainedModelConfig.getLocation() == null) {
                listener.onFailure(ExceptionsHelper.serverError("model [{}] does not have location", trainedModelConfig.getModelId()));
                return;
            }
            validateModelDefinition(
                trainedModelConfig,
                ActionListener.wrap(validate -> getModelBytes(trainedModelConfig, ActionListener.wrap(modelBytes -> {
                    TaskParams taskParams = new TaskParams(
                        trainedModelConfig.getModelId(),
                        modelBytes,
                        request.getThreadsPerAllocation(),
                        request.getNumberOfAllocations(),
                        request.getQueueCapacity(),
                        Optional.ofNullable(request.getCacheSize()).orElse(ByteSizeValue.ofBytes(modelBytes))
                    );
                    PersistentTasksCustomMetadata persistentTasks = clusterService.state()
                        .getMetadata()
                        .custom(PersistentTasksCustomMetadata.TYPE);
                    memoryTracker.refresh(
                        persistentTasks,
                        ActionListener.wrap(
                            aVoid -> trainedModelAssignmentService.createNewModelAssignment(taskParams, waitForDeploymentToStart),
                            listener::onFailure
                        )
                    );
                }, listener::onFailure)), listener::onFailure)
            );

        }, listener::onFailure);

        GetTrainedModelsAction.Request getModelRequest = new GetTrainedModelsAction.Request(request.getModelId());
        client.execute(GetTrainedModelsAction.INSTANCE, getModelRequest, getModelListener);
    }

    private void getModelBytes(TrainedModelConfig trainedModelConfig, ActionListener<Long> listener) {
        ChunkedTrainedModelRestorer restorer = new ChunkedTrainedModelRestorer(
            trainedModelConfig.getModelId(),
            client,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME),
            xContentRegistry
        );
        restorer.setSearchIndex(trainedModelConfig.getLocation().getResourceName());
        restorer.setSearchSize(1);
        restorer.restoreModelDefinition(doc -> {
            // The in-memory size of the model was found to be approximately equal
            // to the size of the model on disk in experiments for BERT models. However,
            // this might not always be the case.
            // TODO Improve heuristic for in-memory model size.
            listener.onResponse(doc.getTotalDefinitionLength());

            // Return false to stop the restorer as we only need the first doc
            return false;
        }, success -> { /* nothing to do */ }, listener::onFailure);
    }

    private void waitForDeploymentState(
        String modelId,
        TimeValue timeout,
        AllocationStatus.State state,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) {
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate(modelId, state, maxLazyMLNodes, maxMLNodeSize);
        trainedModelAssignmentService.waitForAssignmentCondition(
            modelId,
            predicate,
            timeout,
            new TrainedModelAssignmentService.WaitForAssignmentListener() {
                @Override
                public void onResponse(TrainedModelAssignment assignment) {
                    if (predicate.exception != null) {
                        deleteFailedDeployment(modelId, predicate.exception, listener);
                    } else {
                        auditor.info(assignment.getModelId(), Messages.INFERENCE_DEPLOYMENT_STARTED);
                        listener.onResponse(new CreateTrainedModelAssignmentAction.Response(assignment));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    private void deleteFailedDeployment(
        String modelId,
        Exception exception,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) {
        logger.trace(() -> format("[{}] Deleting failed deployment", modelId), exception);
        trainedModelAssignmentService.deleteModelAssignment(modelId, ActionListener.wrap(pTask -> listener.onFailure(exception), e -> {
            logger.error(
                () -> format(
                    "[%s] Failed to delete model allocation that had failed with the reason [%s]",
                    modelId,
                    exception.getMessage()
                ),
                e
            );
            listener.onFailure(exception);
        }));

    }

    private void validateModelDefinition(TrainedModelConfig config, ActionListener<Void> listener) {
        if (config.getLocation() instanceof IndexLocation == false) {
            listener.onResponse(null);
            return;
        }
        final String modelId = config.getModelId();
        final String[] requiredSourceFields = new String[] {
            TrainedModelDefinitionDoc.DEFINITION_LENGTH.getPreferredName(),
            TrainedModelDefinitionDoc.DOC_NUM.getPreferredName(),
            TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName(),
            TrainedModelDefinitionDoc.EOS.getPreferredName() };
        final Set<String> requiredSet = Set.of(requiredSourceFields);
        String index = ((IndexLocation) config.getLocation()).getIndexName();
        client.prepareSearch(index)
            .setQuery(
                QueryBuilders.constantScoreQuery(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId))
                        .filter(
                            QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(), TrainedModelDefinitionDoc.NAME)
                        )
                )
            )
            .setFetchSource(requiredSourceFields, new String[0])
            .setSize(MAX_NUM_NATIVE_DEFINITION_PARTS)
            .setTrackTotalHits(true)
            .addSort(SortBuilders.fieldSort(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName()).order(SortOrder.ASC).unmappedType("long"))
            .execute(ActionListener.wrap(response -> {
                SearchHit[] hits = response.getHits().getHits();
                if (hits.length == 0) {
                    listener.onFailure(new ResourceNotFoundException(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                    return;
                }
                long firstTotalLength = ((Number) hits[0].getSourceAsMap()
                    .get(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName())).longValue();

                long summedLengths = 0;
                for (SearchHit hit : hits) {
                    Map<String, Object> fields = hit.getSourceAsMap();
                    if (fields == null) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                "[{}] model definition [{}] is missing required fields {}. {}",
                                modelId,
                                TrainedModelDefinitionDoc.docNum(modelId, Objects.requireNonNull(hit.getId())),
                                List.of(requiredSourceFields),
                                Messages.UNABLE_TO_DEPLOY_MODEL_BAD_PARTS
                            )
                        );
                        return;
                    }
                    Set<String> diff = Sets.difference(fields.keySet(), requiredSet);
                    if (diff.isEmpty() == false) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                "[{}] model definition [{}] is missing required fields {}. {}",
                                modelId,
                                TrainedModelDefinitionDoc.docNum(modelId, Objects.requireNonNull(hit.getId())),
                                diff,
                                Messages.UNABLE_TO_DEPLOY_MODEL_BAD_PARTS
                            )
                        );
                        return;
                    }
                    summedLengths += ((Number) fields.get(TrainedModelDefinitionDoc.DEFINITION_LENGTH.getPreferredName())).longValue();
                    long totalLength = ((Number) fields.get(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName()))
                        .longValue();
                    if (totalLength != firstTotalLength) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                "[{}] [total_definition_length] must be the same in all model definition parts. "
                                    + "The value [{}] in model definition part [{}] does not match the value [{}] in part [{}]. "
                                    + Messages.UNABLE_TO_DEPLOY_MODEL_BAD_PARTS,
                                modelId,
                                totalLength,
                                TrainedModelDefinitionDoc.docNum(modelId, Objects.requireNonNull(hit.getId())),
                                firstTotalLength,
                                TrainedModelDefinitionDoc.docNum(modelId, Objects.requireNonNull(hits[0].getId()))
                            )
                        );
                        return;
                    }

                }
                Boolean eos = (Boolean) hits[hits.length - 1].getSourceAsMap().get(TrainedModelDefinitionDoc.EOS.getPreferredName());
                if (summedLengths != firstTotalLength || eos == null || eos == false) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(Messages.getMessage(Messages.MODEL_DEFINITION_TRUNCATED, modelId))
                    );
                    return;
                }
                listener.onResponse(null);
            }, e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    Exception ex = new ResourceNotFoundException(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId));
                    ex.addSuppressed(e);
                    listener.onFailure(ex);
                    return;
                }
                listener.onFailure(e);
            }));
    }

    @Override
    protected ClusterBlockException checkBlock(StartTrainedModelDeploymentAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private static class DeploymentStartedPredicate implements Predicate<ClusterState> {

        private volatile Exception exception;

        // for logging
        private final String modelId;
        private final AllocationStatus.State waitForState;
        private final int maxLazyMLNodes;
        private final long maxMLNodeSize;

        DeploymentStartedPredicate(String modelId, AllocationStatus.State waitForState, int maxLazyMLNodes, long maxMLNodeSize) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, "model_id");
            this.waitForState = waitForState;
            this.maxLazyMLNodes = maxLazyMLNodes;
            this.maxMLNodeSize = maxMLNodeSize;
        }

        @Override
        public boolean test(ClusterState clusterState) {
            TrainedModelAssignment trainedModelAssignment = TrainedModelAssignmentMetadata.assignmentForModelId(clusterState, modelId)
                .orElse(null);
            if (trainedModelAssignment == null) {
                // Something weird happened, it should NEVER be null...
                logger.trace(() -> format("[%s] assignment was null while waiting for state [%s]", modelId, waitForState));
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
            boolean isScalingPossible = isScalingPossible(nodes);

            // No nodes allocated at all!
            if (nodeIdsAndRouting.isEmpty() && isScalingPossible == false) {
                String msg = "Could not start deployment because no suitable nodes were found, allocation explanation ["
                    + trainedModelAssignment.getReason()
                    + "]";
                logger.warn("[{}] {}", modelId, msg);
                Exception detail = new IllegalStateException(msg);
                exception = new ElasticsearchStatusException(
                    "Could not start deployment because no ML nodes with sufficient capacity were found",
                    RestStatus.TOO_MANY_REQUESTS,
                    detail
                );
                return true;
            }

            // We cannot add more nodes and the assignment is not satisfied
            if (isScalingPossible == false
                && trainedModelAssignment.isSatisfied(nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet())) == false) {
                String msg = "Could not start deployment because there are not enough resources to provide all requested allocations";
                logger.debug(() -> format("[%s] %s", modelId, msg));
                exception = new ElasticsearchStatusException(msg, RestStatus.TOO_MANY_REQUESTS);
                return true;
            }

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
                    modelId,
                    trainedModelAssignment.getAssignmentState(),
                    nodesStillInitializing
                )
            );
            return false;
        }

        private boolean isScalingPossible(List<DiscoveryNode> nodes) {
            OptionalLong smallestMLNode = nodes.stream().map(NodeLoadDetector::getNodeSize).flatMapToLong(OptionalLong::stream).min();

            // We can scale horizontally
            return maxLazyMLNodes > nodes.size()
                // We can scale vertically
                // TODO this currently only considers memory. We should also consider CPU when autoscaling by CPU is possible.
                || (smallestMLNode.isEmpty() == false && smallestMLNode.getAsLong() < maxMLNodeSize);
        }
    }

    static Set<String> nodesShuttingDown(final ClusterState state) {
        return NodesShutdownMetadata.getShutdowns(state)
            .map(NodesShutdownMetadata::getAllNodeMetadataMap)
            .map(Map::keySet)
            .orElse(Collections.emptySet());
    }

}
