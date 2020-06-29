/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.common.io.stream.StreamInput;
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
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
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
            indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.trainedModelProvider = trainedModelProvider;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response read(StreamInput in) throws IOException {
        return new Response(in);
    }

    @Override
    protected void masterOperation(Task task,
                                   PutTrainedModelAction.Request request,
                                   ClusterState state,
                                   ActionListener<Response> listener) {
        // 7.8.0 introduced splitting the model definition across multiple documents.
        // This means that new models will not be usable on nodes that cannot handle multiple definition documents
        if (state.nodes().getMinNodeVersion().before(Version.V_7_8_0)) {
            listener.onFailure(ExceptionsHelper.badRequestException(
                "Creating a new model requires that all nodes are at least version [{}]",
                request.getTrainedModelConfig().getModelId(),
                Version.V_7_8_0.toString()));
            return;
        }
        try {
            request.getTrainedModelConfig().ensureParsedDefinition(xContentRegistry);
            request.getTrainedModelConfig().getModelDefinition().getTrainedModel().validate();
        } catch (IOException ex) {
            listener.onFailure(ExceptionsHelper.badRequestException("Failed to parse definition for [{}]",
                ex,
                request.getTrainedModelConfig().getModelId()));
            return;
        } catch (ElasticsearchException ex) {
            listener.onFailure(ExceptionsHelper.badRequestException("Definition for [{}] has validation failures.",
                ex,
                request.getTrainedModelConfig().getModelId()));
            return;
        }
        if (request.getTrainedModelConfig()
            .getInferenceConfig()
            .isTargetTypeSupported(request.getTrainedModelConfig()
                .getModelDefinition()
                .getTrainedModel()
                .targetType()) == false) {
            listener.onFailure(ExceptionsHelper.badRequestException(
                "Model [{}] inference config type [{}] does not support definition target type [{}]",
                request.getTrainedModelConfig().getModelId(),
                request.getTrainedModelConfig().getInferenceConfig().getName(),
                request.getTrainedModelConfig()
                    .getModelDefinition()
                    .getTrainedModel()
                    .targetType()));
            return;
        }

        Version minCompatibilityVersion = request.getTrainedModelConfig()
            .getModelDefinition()
            .getTrainedModel()
            .getMinimalCompatibilityVersion();
        if (state.nodes().getMinNodeVersion().before(minCompatibilityVersion)) {
            listener.onFailure(ExceptionsHelper.badRequestException(
                "Definition for [{}] requires that all nodes are at least version [{}]",
                request.getTrainedModelConfig().getModelId(),
                minCompatibilityVersion.toString()));
            return;
        }

        TrainedModelConfig trainedModelConfig = new TrainedModelConfig.Builder(request.getTrainedModelConfig())
            .setVersion(Version.CURRENT)
            .setCreateTime(Instant.now())
            .setCreatedBy("api_user")
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setEstimatedHeapMemory(request.getTrainedModelConfig().getModelDefinition().ramBytesUsed())
            .setEstimatedOperations(request.getTrainedModelConfig().getModelDefinition().getTrainedModel().estimatedNumOperations())
            .build();

        ActionListener<Void> tagsModelIdCheckListener = ActionListener.wrap(
            r -> trainedModelProvider.storeTrainedModel(trainedModelConfig, ActionListener.wrap(
                bool -> {
                    TrainedModelConfig configToReturn = new TrainedModelConfig.Builder(trainedModelConfig).clearDefinition().build();
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

        checkModelIdAgainstTags(request.getTrainedModelConfig().getModelId(), modelIdTagCheckListener);
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
        if (licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
