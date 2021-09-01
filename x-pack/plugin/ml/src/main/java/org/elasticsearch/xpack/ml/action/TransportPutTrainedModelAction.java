/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
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

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportPutTrainedModelAction extends TransportMasterNodeAction<Request, Response> {

    private final TrainedModelProvider trainedModelProvider;
    private final XPackLicenseState licenseState;
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;

    @Inject
    public TransportPutTrainedModelAction(TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, XPackLicenseState licenseState, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver, Client client,
                                          TrainedModelProvider trainedModelProvider, NamedXContentRegistry xContentRegistry) {
        super(PutTrainedModelAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new,
            indexNameExpressionResolver, Response::new, ThreadPool.Names.SAME);
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task,
                                   PutTrainedModelAction.Request request,
                                   ClusterState state,
                                   ActionListener<Response> listener) {
        TrainedModelConfig config = request.getTrainedModelConfig();
        try {
            config.ensureParsedDefinition(xContentRegistry);
        } catch (IOException ex) {
            listener.onFailure(ExceptionsHelper.badRequestException("Failed to parse definition for [{}]",
                ex,
                config.getModelId()));
            return;
        }

        boolean hasModelDefinition = config.getModelDefinition() != null;
        if (hasModelDefinition) {
            try {
                config.getModelDefinition().getTrainedModel().validate();
            } catch (ElasticsearchException ex) {
                listener.onFailure(ExceptionsHelper.badRequestException("Definition for [{}] has validation failures.",
                    ex,
                    config.getModelId()));
                return;
            }

            TrainedModelType trainedModelType =
                TrainedModelType.typeFromTrainedModel(config.getModelDefinition().getTrainedModel());
            if (trainedModelType == null) {
                listener.onFailure(ExceptionsHelper.badRequestException("Unknown trained model definition class [{}]",
                    config.getModelDefinition().getTrainedModel().getName()));
                return;
            }

            if (config.getModelType() == null) {
                // Set the model type from the definition
                config = new TrainedModelConfig.Builder(config).setModelType(trainedModelType).build();
            } else if (trainedModelType != config.getModelType()) {
                listener.onFailure(ExceptionsHelper.badRequestException(
                    "{} [{}] does not match the model definition type [{}]",
                    TrainedModelConfig.MODEL_TYPE.getPreferredName(), config.getModelType(),
                    trainedModelType));
                return;
            }

            if (config.getInferenceConfig()
                .isTargetTypeSupported(config
                    .getModelDefinition()
                    .getTrainedModel()
                    .targetType()) == false) {
                listener.onFailure(ExceptionsHelper.badRequestException(
                    "Model [{}] inference config type [{}] does not support definition target type [{}]",
                    config.getModelId(),
                    config.getInferenceConfig().getName(),
                    config.getModelDefinition().getTrainedModel().targetType()));
                return;
            }

            Version minCompatibilityVersion = config
                .getModelDefinition()
                .getTrainedModel()
                .getMinimalCompatibilityVersion();
            if (state.nodes().getMinNodeVersion().before(minCompatibilityVersion)) {
                listener.onFailure(ExceptionsHelper.badRequestException(
                    "Definition for [{}] requires that all nodes are at least version [{}]",
                    config.getModelId(),
                    minCompatibilityVersion.toString()));
                return;
            }
        }




        TrainedModelConfig.Builder trainedModelConfig = new TrainedModelConfig.Builder(config)
            .setVersion(Version.CURRENT)
            .setCreateTime(Instant.now())
            .setCreatedBy("api_user")
            .setLicenseLevel(License.OperationMode.PLATINUM.description());
        if (hasModelDefinition) {
            trainedModelConfig.setEstimatedHeapMemory(config.getModelDefinition().ramBytesUsed())
                .setEstimatedOperations(config.getModelDefinition().getTrainedModel().estimatedNumOperations());
        }

        if (ModelAliasMetadata.fromState(state).getModelId(trainedModelConfig.getModelId()) != null) {
            listener.onFailure(ExceptionsHelper.badRequestException(
                "requested model_id [{}] is the same as an existing model_alias. Model model_aliases and ids must be unique",
                config.getModelId()
            ));
            return;
        }

        ActionListener<Void> tagsModelIdCheckListener = ActionListener.wrap(
            r -> trainedModelProvider.storeTrainedModel(trainedModelConfig.build(), ActionListener.wrap(
                bool -> {
                    TrainedModelConfig configToReturn = trainedModelConfig.clearDefinition().build();
                    listener.onResponse(new PutTrainedModelAction.Response(configToReturn));
                },
                listener::onFailure
            )),
            listener::onFailure
        );

        ActionListener<Void> modelIdTagCheckListener = ActionListener.wrap(
            r -> checkTagsAgainstModelIds(request.getTrainedModelConfig().getTags(), tagsModelIdCheckListener),
            listener::onFailure
        );

        checkModelIdAgainstTags(config.getModelId(), modelIdTagCheckListener);
    }

    private void checkModelIdAgainstTags(String modelId, ActionListener<Void> listener) {
        QueryBuilder builder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(TrainedModelConfig.TAGS.getPreferredName(), modelId)));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(builder).size(0).trackTotalHitsUpTo(1);
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN).source(sourceBuilder);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(
                response -> {
                    if (response.getHits().getTotalHits().value > 0) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(
                                Messages.getMessage(Messages.INFERENCE_MODEL_ID_AND_TAGS_UNIQUE, modelId)));
                        return;
                    }
                    listener.onResponse(null);
                },
                listener::onFailure
            ),
            client::search);
    }

    private void checkTagsAgainstModelIds(List<String> tags, ActionListener<Void> listener) {
        if (tags.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        QueryBuilder builder = QueryBuilders.constantScoreQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), tags)));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(builder).size(0).trackTotalHitsUpTo(1);
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN).source(sourceBuilder);
        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(
                response -> {
                    if (response.getHits().getTotalHits().value > 0) {
                        listener.onFailure(
                            ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INFERENCE_TAGS_AND_MODEL_IDS_UNIQUE, tags)));
                        return;
                    }
                    listener.onResponse(null);
                },
                listener::onFailure
            ),
            client::search);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
