/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.TRAINED_MODEL_INPUTS_DIFFER_SIGNIFICANTLY;

public class TransportPutTrainedModelAliasAction extends AcknowledgedTransportMasterNodeAction<PutTrainedModelAliasAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportPutTrainedModelAliasAction.class);

    private final XPackLicenseState licenseState;
    private final TrainedModelProvider trainedModelProvider;
    private final InferenceAuditor auditor;

    @Inject
    public TransportPutTrainedModelAliasAction(
        TransportService transportService,
        TrainedModelProvider trainedModelProvider,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ActionFilters actionFilters,
        InferenceAuditor auditor,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutTrainedModelAliasAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutTrainedModelAliasAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.auditor = auditor;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutTrainedModelAliasAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        final boolean mlSupported = MachineLearningField.ML_API_FEATURE.check(licenseState);
        final Predicate<TrainedModelConfig> isLicensed = (model) -> mlSupported
            // Either we support plat+ or the model is basic licensed
            || model.getLicenseLevel() == License.OperationMode.BASIC;
        final String oldModelId = ModelAliasMetadata.fromState(state).getModelId(request.getModelAlias());

        if (oldModelId != null && (request.isReassign() == false)) {
            listener.onFailure(
                ExceptionsHelper.badRequestException(
                    "cannot assign model_alias [{}] to model_id [{}] as model_alias already refers to [{}]. "
                        + "Set parameter [reassign] to [true] if model_alias should be reassigned.",
                    request.getModelAlias(),
                    request.getModelId(),
                    oldModelId
                )
            );
            return;
        }
        Set<String> modelIds = new HashSet<>();
        modelIds.add(request.getModelAlias());
        modelIds.add(request.getModelId());
        if (oldModelId != null) {
            modelIds.add(oldModelId);
        }
        trainedModelProvider.getTrainedModels(modelIds, GetTrainedModelsAction.Includes.empty(), true, null, ActionListener.wrap(models -> {
            TrainedModelConfig newModel = null;
            TrainedModelConfig oldModel = null;
            for (TrainedModelConfig config : models) {
                if (config.getModelId().equals(request.getModelId())) {
                    newModel = config;
                }
                if (config.getModelId().equals(oldModelId)) {
                    oldModel = config;
                }
                if (config.getModelId().equals(request.getModelAlias())) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException("model_alias cannot be the same as an existing trained model_id")
                    );
                    return;
                }
            }
            if (newModel == null) {
                listener.onFailure(ExceptionsHelper.missingTrainedModel(request.getModelId()));
                return;
            }
            if (isLicensed.test(newModel) == false) {
                listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
                return;
            }
            // if old model is null, none of these validations matter
            // we should still allow reassignment even if the old model was some how deleted and the alias still refers to it
            if (oldModel != null) {
                // validate inference configs are the same type. Moving an alias from regression -> classification seems dangerous
                if (newModel.getInferenceConfig() != null && oldModel.getInferenceConfig() != null) {
                    if (newModel.getInferenceConfig().getName().equals(oldModel.getInferenceConfig().getName()) == false) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                "cannot reassign model_alias [{}] to model [{}] "
                                    + "with inference config type [{}] from model [{}] with type [{}]",
                                request.getModelAlias(),
                                newModel.getModelId(),
                                newModel.getInferenceConfig().getName(),
                                oldModel.getModelId(),
                                oldModel.getInferenceConfig().getName()
                            )
                        );
                        return;
                    }
                }

                if (Objects.equals(newModel.getModelType(), oldModel.getModelType()) == false) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(
                            "cannot reassign model_alias [{}] to model [{}] with type [{}] from model [{}] with type [{}]",
                            request.getModelAlias(),
                            newModel.getModelId(),
                            Optional.ofNullable(newModel.getModelType()).orElse(TrainedModelType.TREE_ENSEMBLE).toString(),
                            oldModel.getModelId(),
                            Optional.ofNullable(oldModel.getModelType()).orElse(TrainedModelType.TREE_ENSEMBLE).toString()
                        )
                    );
                    return;
                }

                // If we are reassigning Pytorch models, we need to validate assignments are acceptable.
                if (newModel.getModelType() == TrainedModelType.PYTORCH) {
                    List<TrainedModelAssignment> oldAssignments = TrainedModelAssignmentMetadata.assignmentsForModelId(state, oldModelId);
                    List<TrainedModelAssignment> newAssignments = TrainedModelAssignmentMetadata.assignmentsForModelId(
                        state,
                        newModel.getModelId()
                    );
                    // Old model is currently deployed
                    if (oldAssignments.isEmpty() == false) {
                        // disallow changing the model alias from a deployed model to an undeployed model
                        if (newAssignments.isEmpty()) {
                            listener.onFailure(
                                ExceptionsHelper.badRequestException(
                                    "cannot reassign model_alias [{}] to model [{}] from model [{}] as it is not yet deployed",
                                    request.getModelAlias(),
                                    newModel.getModelId(),
                                    oldModel.getModelId()
                                )
                            );
                            return;
                        } else {
                            for (var oldAssignment : oldAssignments) {
                                Optional<AllocationStatus> oldAllocationStatus = oldAssignment.calculateAllocationStatus();
                                // Old model is deployed and its allocation status is NOT "stopping" or "starting"
                                if (oldAllocationStatus.isPresent()
                                    && oldAllocationStatus.get()
                                        .calculateState()
                                        .isAnyOf(AllocationStatus.State.FULLY_ALLOCATED, AllocationStatus.State.STARTED)) {
                                    for (var newAssignment : newAssignments) {
                                        Optional<AllocationStatus> newAllocationStatus = newAssignment.calculateAllocationStatus();
                                        if (newAllocationStatus.isEmpty()
                                            || newAllocationStatus.get().calculateState().equals(AllocationStatus.State.STARTING)) {
                                            listener.onFailure(
                                                ExceptionsHelper.badRequestException(
                                                    "cannot reassign model_alias [{}] to model [{}] "
                                                        + " from model [{}] as it is not yet allocated to any nodes",
                                                    request.getModelAlias(),
                                                    newModel.getModelId(),
                                                    oldModel.getModelId()
                                                )
                                            );
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                Set<String> oldInputFields = new HashSet<>(oldModel.getInput().getFieldNames());
                Set<String> newInputFields = new HashSet<>(newModel.getInput().getFieldNames());
                // TODO should we fail in this case???
                if (Sets.difference(oldInputFields, newInputFields).size() > (oldInputFields.size() / 2)
                    || Sets.intersection(newInputFields, oldInputFields).size() < (oldInputFields.size() / 2)) {
                    String warning = Messages.getMessage(TRAINED_MODEL_INPUTS_DIFFER_SIGNIFICANTLY, request.getModelId(), oldModelId);
                    auditor.warning(oldModelId, warning);
                    logger.warn("[{}] {}", oldModelId, warning);
                    HeaderWarning.addWarning(warning);
                }
            }
            submitUnbatchedTask("update-model-alias", new AckedClusterStateUpdateTask(request, listener) {
                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return updateModelAlias(currentState, request);
                }
            });

        }, listener::onFailure));
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static ClusterState updateModelAlias(final ClusterState currentState, final PutTrainedModelAliasAction.Request request) {
        final ClusterState.Builder builder = ClusterState.builder(currentState);
        final ModelAliasMetadata currentMetadata = ModelAliasMetadata.fromState(currentState);
        String currentModelId = currentMetadata.getModelId(request.getModelAlias());
        final Map<String, ModelAliasMetadata.ModelAliasEntry> newMetadata = new HashMap<>(currentMetadata.modelAliases());
        if (currentModelId == null) {
            logger.info("creating new model_alias [{}] for model [{}]", request.getModelAlias(), request.getModelId());
        } else {
            logger.info(
                "updating model_alias [{}] to refer to model [{}] from model [{}]",
                request.getModelAlias(),
                request.getModelId(),
                currentModelId
            );
        }
        newMetadata.put(request.getModelAlias(), new ModelAliasMetadata.ModelAliasEntry(request.getModelId()));
        final ModelAliasMetadata modelAliasMetadata = new ModelAliasMetadata(newMetadata);
        builder.metadata(Metadata.builder(currentState.getMetadata()).putCustom(ModelAliasMetadata.NAME, modelAliasMetadata).build());
        return builder.build();
    }

    @Override
    protected ClusterBlockException checkBlock(PutTrainedModelAliasAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
