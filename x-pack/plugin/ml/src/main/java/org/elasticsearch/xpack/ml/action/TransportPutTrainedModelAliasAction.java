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
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
        IndexNameExpressionResolver indexNameExpressionResolver) {
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
        final boolean mlSupported = licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING);
        final Predicate<TrainedModelConfig> isLicensed = (model) -> mlSupported || licenseState.isAllowedByLicense(model.getLicenseLevel());
        final String oldModelId = ModelAliasMetadata.fromState(state).getModelId(request.getModelAlias());

        if (oldModelId != null && (request.isReassign() == false)) {
            listener.onFailure(ExceptionsHelper.badRequestException(
                "cannot assign model_alias [{}] to model_id [{}] as model_alias already refers to [{}]. "
                    +
                    "Set parameter [reassign] to [true] if model_alias should be reassigned.",
                request.getModelAlias(),
                request.getModelId(),
                oldModelId));
            return;
        }
        Set<String> modelIds = new HashSet<>();
        modelIds.add(request.getModelAlias());
        modelIds.add(request.getModelId());
        if (oldModelId != null) {
            modelIds.add(oldModelId);
        }
        trainedModelProvider.getTrainedModels(modelIds, GetTrainedModelsAction.Includes.empty(), true, ActionListener.wrap(
            models -> {
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
                    listener.onFailure(
                        ExceptionsHelper.missingTrainedModel(request.getModelId())
                    );
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

                    Set<String> oldInputFields = new HashSet<>(oldModel.getInput().getFieldNames());
                    Set<String> newInputFields = new HashSet<>(newModel.getInput().getFieldNames());
                    // TODO should we fail in this case???
                    if (Sets.difference(oldInputFields, newInputFields).size() > (oldInputFields.size() / 2)
                    || Sets.intersection(newInputFields, oldInputFields).size() < (oldInputFields.size() / 2)) {
                        String warning =  Messages.getMessage(
                            TRAINED_MODEL_INPUTS_DIFFER_SIGNIFICANTLY,
                            request.getModelId(),
                            oldModelId);
                        auditor.warning(oldModelId, warning);
                        logger.warn("[{}] {}", oldModelId, warning);
                        HeaderWarning.addWarning(warning);
                    }
                }
                clusterService.submitStateUpdateTask("update-model-alias", new AckedClusterStateUpdateTask(request, listener) {
                    @Override
                    public ClusterState execute(final ClusterState currentState) {
                        return updateModelAlias(currentState, request);
                    }
                });

            },
            listener::onFailure
        ));
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
