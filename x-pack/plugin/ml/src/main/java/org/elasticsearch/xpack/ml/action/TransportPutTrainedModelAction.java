/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.Request;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction.Response;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportPutTrainedModelAction extends TransportMasterNodeAction<Request, Response> {

    private static final ByteSizeValue MAX_NATIVE_DEFINITION_INDEX_SIZE = ByteSizeValue.ofGb(50);

    private final TrainedModelProvider trainedModelProvider;
    private final XPackLicenseState licenseState;
    private final NamedXContentRegistry xContentRegistry;
    private final OriginSettingClient client;

    @Inject
    public TransportPutTrainedModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        TrainedModelProvider trainedModelProvider,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            PutTrainedModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.xContentRegistry = xContentRegistry;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutTrainedModelAction.Request request,
        ClusterState state,
        ActionListener<Response> listener
    ) {
        TrainedModelConfig config = request.getTrainedModelConfig();
        try {
            if (request.isDeferDefinitionDecompression() == false) {
                config.ensureParsedDefinition(xContentRegistry);
            }
        } catch (IOException ex) {
            listener.onFailure(ExceptionsHelper.badRequestException("Failed to parse definition for [{}]", ex, config.getModelId()));
            return;
        }

        // NOTE: hasModelDefinition is false if we don't parse it. But, if the fully parsed model was already provided, continue
        boolean hasModelDefinition = config.getModelDefinition() != null;
        if (hasModelDefinition) {
            try {
                config.getModelDefinition().getTrainedModel().validate();
            } catch (ElasticsearchException ex) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException("Definition for [{}] has validation failures.", ex, config.getModelId())
                );
                return;
            }

            TrainedModelType trainedModelType = TrainedModelType.typeFromTrainedModel(config.getModelDefinition().getTrainedModel());
            if (trainedModelType == null) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Unknown trained model definition class [{}]",
                        config.getModelDefinition().getTrainedModel().getName()
                    )
                );
                return;
            }

            if (config.getModelType() == null) {
                // Set the model type from the definition
                config = new TrainedModelConfig.Builder(config).setModelType(trainedModelType).build();
            } else if (trainedModelType != config.getModelType()) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "{} [{}] does not match the model definition type [{}]",
                        TrainedModelConfig.MODEL_TYPE.getPreferredName(),
                        config.getModelType(),
                        trainedModelType
                    )
                );
                return;
            }

            if (config.getInferenceConfig().isTargetTypeSupported(config.getModelDefinition().getTrainedModel().targetType()) == false) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Model [{}] inference config type [{}] does not support definition target type [{}]",
                        config.getModelId(),
                        config.getInferenceConfig().getName(),
                        config.getModelDefinition().getTrainedModel().targetType()
                    )
                );
                return;
            }

            Version minCompatibilityVersion = config.getModelDefinition().getTrainedModel().getMinimalCompatibilityVersion();
            if (state.nodes().getMinNodeVersion().before(minCompatibilityVersion)) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Definition for [{}] requires that all nodes are at least version [{}]",
                        config.getModelId(),
                        minCompatibilityVersion.toString()
                    )
                );
                return;
            }
        }

        TrainedModelConfig.Builder trainedModelConfig = new TrainedModelConfig.Builder(config).setVersion(Version.CURRENT)
            .setCreateTime(Instant.now())
            .setCreatedBy("api_user")
            .setLicenseLevel(License.OperationMode.PLATINUM.description());
        if (hasModelDefinition) {
            trainedModelConfig.setModelSize(config.getModelDefinition().ramBytesUsed())
                .setEstimatedOperations(config.getModelDefinition().getTrainedModel().estimatedNumOperations());
        } else {
            // Set default location for the given model type.
            trainedModelConfig.setLocation(
                Optional.ofNullable(config.getModelType()).orElse(TrainedModelType.TREE_ENSEMBLE).getDefaultLocation(config.getModelId())
            );
        }

        if (ModelAliasMetadata.fromState(state).getModelId(trainedModelConfig.getModelId()) != null) {
            listener.onFailure(
                ExceptionsHelper.badRequestException(
                    "requested model_id [{}] is the same as an existing model_alias. Model model_aliases and ids must be unique",
                    config.getModelId()
                )
            );
            return;
        }

        ActionListener<Void> checkStorageIndexSizeListener = ActionListener.wrap(
            r -> trainedModelProvider.storeTrainedModel(trainedModelConfig.build(), ActionListener.wrap(bool -> {
                TrainedModelConfig configToReturn = trainedModelConfig.clearDefinition().build();
                listener.onResponse(new PutTrainedModelAction.Response(configToReturn));
            }, listener::onFailure)),
            listener::onFailure
        );

        ActionListener<Void> tagsModelIdCheckListener = ActionListener.wrap(r -> {
            if (TrainedModelType.PYTORCH.equals(trainedModelConfig.getModelType())) {
                client.admin()
                    .indices()
                    .prepareStats(InferenceIndexConstants.nativeDefinitionStore())
                    .clear()
                    .setStore(true)
                    .execute(ActionListener.wrap(stats -> {
                        IndexStats indexStats = stats.getIndices().get(InferenceIndexConstants.nativeDefinitionStore());
                        if (indexStats == null) {
                            checkStorageIndexSizeListener.onResponse(null);
                            return;
                        }
                        if (indexStats.getTotal().getStore().getSizeInBytes() > MAX_NATIVE_DEFINITION_INDEX_SIZE.getBytes()) {
                            listener.onFailure(
                                new ElasticsearchStatusException(
                                    "Native model store has exceeded the maximum acceptable size of {}, "
                                        + "please delete older unused pytorch models",
                                    RestStatus.CONFLICT,
                                    MAX_NATIVE_DEFINITION_INDEX_SIZE.toString()
                                )
                            );
                            return;
                        }
                        checkStorageIndexSizeListener.onResponse(null);
                    }, e -> {
                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                            checkStorageIndexSizeListener.onResponse(null);
                            return;
                        }
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                "Unable to calculate stats for definition storage index [{}], please try again later",
                                RestStatus.SERVICE_UNAVAILABLE,
                                e,
                                InferenceIndexConstants.nativeDefinitionStore()
                            )
                        );
                    }));
                return;
            }
            checkStorageIndexSizeListener.onResponse(null);
        }, listener::onFailure);

        ActionListener<Void> modelIdTagCheckListener = ActionListener.wrap(
            r -> checkTagsAgainstModelIds(request.getTrainedModelConfig().getTags(), tagsModelIdCheckListener),
            listener::onFailure
        );

        checkModelIdAgainstTags(config.getModelId(), modelIdTagCheckListener);
    }

    private void checkModelIdAgainstTags(String modelId, ActionListener<Void> listener) {
        QueryBuilder builder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(TrainedModelConfig.TAGS.getPreferredName(), modelId))
        );
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(builder).size(0).trackTotalHitsUpTo(1);
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN).source(sourceBuilder);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                if (response.getHits().getTotalHits().value > 0) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INFERENCE_MODEL_ID_AND_TAGS_UNIQUE, modelId))
                    );
                    return;
                }
                listener.onResponse(null);
            }, listener::onFailure),
            client::search
        );
    }

    private void checkTagsAgainstModelIds(List<String> tags, ActionListener<Void> listener) {
        if (tags.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        QueryBuilder builder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), tags))
        );
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(builder).size(0).trackTotalHitsUpTo(1);
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN).source(sourceBuilder);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                if (response.getHits().getTotalHits().value > 0) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INFERENCE_TAGS_AND_MODEL_IDS_UNIQUE, tags))
                    );
                    return;
                }
                listener.onResponse(null);
            }, listener::onFailure),
            client::search
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
