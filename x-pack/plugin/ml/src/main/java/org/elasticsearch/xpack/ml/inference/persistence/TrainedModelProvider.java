/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TrainedModelProvider {

    private static final Logger logger = LogManager.getLogger(TrainedModelProvider.class);
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private static final ToXContent.Params FOR_INTERNAL_STORAGE_PARAMS =
        new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));

    public TrainedModelProvider(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    public void storeTrainedModel(TrainedModelConfig trainedModelConfig, ActionListener<Boolean> listener) {

        ActionListener<IndexResponse> putDefinitionListener = ActionListener.wrap(
            r -> listener.onResponse(true),
            e -> {
                logger.error(new ParameterizedMessage(
                    "[{}] failed to store trained model definition for inference", trainedModelConfig.getModelId()), e);
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                    listener.onFailure(new ResourceAlreadyExistsException(
                        Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelConfig.getModelId())));
                } else {
                    listener.onFailure(
                        new ElasticsearchStatusException(Messages.INFERENCE_FAILED_TO_STORE_MODEL,
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e,
                            trainedModelConfig.getModelId()));
                }
            }
        );

        ActionListener<IndexResponse> putConfigListener = ActionListener.wrap(
            r -> {
                if (trainedModelConfig.getDefinition() != null) {
                    indexObject(TrainedModelDefinition.docId(trainedModelConfig.getModelId()),
                        trainedModelConfig.getDefinition(),
                        putDefinitionListener);
                } else {
                    listener.onResponse(true);
                }
            },
            e -> {
                logger.error(new ParameterizedMessage(
                    "[{}] failed to store trained model for inference", trainedModelConfig.getModelId()), e);
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                    listener.onFailure(new ResourceAlreadyExistsException(
                        Messages.getMessage(Messages.INFERENCE_TRAINED_MODEL_EXISTS, trainedModelConfig.getModelId())));
                } else {
                    listener.onFailure(
                        new ElasticsearchStatusException(Messages.INFERENCE_FAILED_TO_STORE_MODEL,
                            RestStatus.INTERNAL_SERVER_ERROR,
                            e,
                            trainedModelConfig.getModelId()));
                }
            }
        );

        indexObject(trainedModelConfig.getModelId(), trainedModelConfig, putConfigListener);
    }

    public void getTrainedModel(final String modelId, final boolean includeDefinition, final ActionListener<TrainedModelConfig> listener) {

        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders
            .idsQuery()
            .addIds(modelId));
        MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch()
            .add(client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
                .setQuery(queryBuilder)
                // use sort to get the last
                .addSort("_index", SortOrder.DESC)
                .setSize(1)
                .request());

        if (includeDefinition) {
            multiSearchRequestBuilder.add(client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
                .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders
                    .idsQuery()
                    .addIds(TrainedModelDefinition.docId(modelId))))
                // use sort to get the last
                .addSort("_index", SortOrder.DESC)
                .setSize(1)
                .request());
        }

        ActionListener<MultiSearchResponse> multiSearchResponseActionListener = ActionListener.wrap(
            multiSearchResponse -> {
                TrainedModelConfig.Builder builder;
                TrainedModelDefinition definition;
                try {
                    builder = handleSearchItem(multiSearchResponse.getResponses()[0], modelId, this::parseInferenceDocLenientlyFromSource);
                } catch(ResourceNotFoundException ex) {
                    listener.onFailure(new ResourceNotFoundException(
                        Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
                    return;
                } catch (Exception ex) {
                    listener.onFailure(ex);
                    return;
                }

                if (includeDefinition) {
                    try {
                        definition = handleSearchItem(multiSearchResponse.getResponses()[1],
                            modelId,
                            this::parseModelDefinitionDocLenientlyFromSource);
                        builder.setDefinition(definition);
                    } catch(ResourceNotFoundException ex) {
                        listener.onFailure(new ResourceNotFoundException(
                            Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                        return;
                    } catch (Exception ex) {
                        listener.onFailure(ex);
                        return;
                    }
                }
                listener.onResponse(builder.build());
            },
            listener::onFailure
        );

        executeAsyncWithOrigin(client,
            ML_ORIGIN,
            MultiSearchAction.INSTANCE,
            multiSearchRequestBuilder.request(),
            multiSearchResponseActionListener);
    }


    private static <T> T handleSearchItem(MultiSearchResponse.Item item,
                                          String resourceId,
                                          CheckedBiFunction<BytesReference, String, T, Exception> parseLeniently) throws Exception {
        if (item.isFailure()) {
            throw item.getFailure();
        }
        if (item.getResponse().getHits().getHits().length == 0) {
            throw new ResourceNotFoundException(resourceId);
        }
        return parseLeniently.apply(item.getResponse().getHits().getHits()[0].getSourceRef(), resourceId);
    }

    private TrainedModelConfig.Builder parseInferenceDocLenientlyFromSource(BytesReference source, String modelId) throws Exception {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return TrainedModelConfig.fromXContent(parser, true);
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("[{}] failed to parse model", modelId), e);
            throw e;
        }
    }

    private TrainedModelDefinition parseModelDefinitionDocLenientlyFromSource(BytesReference source, String modelId) throws Exception {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                 .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return TrainedModelDefinition.fromXContent(parser, true).build();
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("[{}] failed to parse model definition", modelId), e);
            throw e;
        }
    }

    private void indexObject(String docId, ToXContentObject body, ActionListener<IndexResponse> indexListener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = body.toXContent(builder, FOR_INTERNAL_STORAGE_PARAMS);

            IndexRequest indexRequest = new IndexRequest(InferenceIndexConstants.LATEST_INDEX_NAME)
                .opType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .id(docId)
                .source(source);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, indexListener);
        } catch (IOException ex) {
            // not expected to happen but for the sake of completeness
            indexListener.onFailure(ex);
        }
    }
}
