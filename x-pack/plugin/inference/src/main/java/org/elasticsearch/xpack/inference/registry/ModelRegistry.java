/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferenceSecretsIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class ModelRegistry {
    public record ModelConfigMap(Map<String, Object> config, Map<String, Object> secrets) {}

    private static final Logger logger = LogManager.getLogger(ModelRegistry.class);
    private final OriginSettingClient client;

    public ModelRegistry(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    public void getUnparsedModelMap(String modelId, ActionListener<ModelConfigMap> listener) {
        ActionListener<SearchResponse> searchListener = ActionListener.wrap(searchResponse -> {
            // There should be a hit for the configurations and secrets
            if (searchResponse.getHits().getHits().length == 0) {
                listener.onFailure(new ResourceNotFoundException("Model not found [{}]", modelId));
                return;
            }

            var hits = searchResponse.getHits().getHits();
            listener.onResponse(createModelConfigMap(hits, modelId));

        }, listener::onFailure);

        QueryBuilder queryBuilder = documentIdQuery(modelId);
        SearchRequest modelSearch = client.prepareSearch(InferenceIndex.INDEX_PATTERN, InferenceSecretsIndex.INDEX_PATTERN)
            .setQuery(queryBuilder)
            .setSize(2)
            .request();

        client.search(modelSearch, searchListener);
    }

    private ModelConfigMap createModelConfigMap(SearchHit[] hits, String modelId) {
        Map<String, SearchHit> mappedHits = Arrays.stream(hits).collect(Collectors.toMap(hit -> {
            if (hit.getIndex().startsWith(InferenceIndex.INDEX_NAME)) {
                return InferenceIndex.INDEX_NAME;
            }

            if (hit.getIndex().startsWith(InferenceSecretsIndex.INDEX_NAME)) {
                return InferenceSecretsIndex.INDEX_NAME;
            }

            logger.warn(format("Found invalid index for model [%s] at index [%s]", modelId, hit.getIndex()));
            throw new IllegalArgumentException(
                format(
                    "Invalid result while loading model [%s] index: [%s]. Try deleting and reinitializing the service",
                    modelId,
                    hit.getIndex()
                )
            );
        }, Function.identity()));

        if (mappedHits.containsKey(InferenceIndex.INDEX_NAME) == false
            || mappedHits.containsKey(InferenceSecretsIndex.INDEX_NAME) == false
            || mappedHits.size() > 2) {
            logger.warn(format("Failed to load model [%s], found model parts from index prefixes: [%s]", modelId, mappedHits.keySet()));
            throw new IllegalStateException(
                format("Failed to load model, model [%s] is in an invalid state. Try deleting and reinitializing the service", modelId)
            );
        }

        return new ModelConfigMap(
            mappedHits.get(InferenceIndex.INDEX_NAME).getSourceAsMap(),
            mappedHits.get(InferenceSecretsIndex.INDEX_NAME).getSourceAsMap()
        );
    }

    public void storeModel(Model model, ActionListener<Boolean> listener) {
        ActionListener<BulkResponse> bulkResponseActionListener = getStoreModelListener(model, listener);

        IndexRequest configRequest = createIndexRequest(
            Model.documentId(model.getConfigurations().getModelId()),
            InferenceIndex.INDEX_NAME,
            model.getConfigurations(),
            false
        );

        IndexRequest secretsRequest = createIndexRequest(
            Model.documentId(model.getConfigurations().getModelId()),
            InferenceSecretsIndex.INDEX_NAME,
            model.getSecrets(),
            false
        );

        client.prepareBulk()
            .add(configRequest)
            .add(secretsRequest)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(bulkResponseActionListener);
    }

    private static ActionListener<BulkResponse> getStoreModelListener(Model model, ActionListener<Boolean> listener) {
        return ActionListener.wrap(bulkItemResponses -> {
            var modelId = model.getConfigurations().getModelId();

            if (bulkItemResponses.getItems().length == 0) {
                logger.warn(format("Storing model [%s] failed, no items were received from the bulk response", modelId));

                listener.onFailure(
                    new ElasticsearchStatusException(
                        format(
                            "Failed to store inference model [%s], invalid bulk response received. Try reinitializing the service",
                            modelId
                        ),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
                return;
            }

            BulkItemResponse.Failure failure = getFirstBulkFailure(bulkItemResponses);

            if (failure == null) {
                listener.onResponse(true);
                return;
            }

            logBulkFailures(model.getConfigurations().getModelId(), bulkItemResponses);

            if (ExceptionsHelper.unwrapCause(failure.getCause()) instanceof VersionConflictEngineException) {
                listener.onFailure(new ResourceAlreadyExistsException("Inference model [{}] already exists", modelId));
                return;
            }

            listener.onFailure(
                new ElasticsearchStatusException(
                    format("Failed to store inference model [%s]", modelId),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    failure.getCause()
                )
            );
        }, e -> {
            String errorMessage = format("Failed to store inference model [%s]", model.getConfigurations().getModelId());
            logger.warn(errorMessage, e);
            listener.onFailure(new ElasticsearchStatusException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR, e));
        });
    }

    private static void logBulkFailures(String modelId, BulkResponse bulkResponse) {
        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                logger.warn(
                    format(
                        "Failed to store inference model [%s] index: [%s] bulk failure message [%s]",
                        modelId,
                        item.getIndex(),
                        item.getFailureMessage()
                    )
                );
            }
        }
    }

    private static BulkItemResponse.Failure getFirstBulkFailure(BulkResponse bulkResponse) {
        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                return item.getFailure();
            }
        }

        return null;
    }

    public void deleteModel(String modelId, ActionListener<Boolean> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest().setAbortOnVersionConflict(false);
        request.indices(InferenceIndex.INDEX_PATTERN, InferenceSecretsIndex.INDEX_PATTERN);
        request.setQuery(documentIdQuery(modelId));
        request.setRefresh(true);

        client.execute(
            DeleteByQueryAction.INSTANCE,
            request,
            ActionListener.wrap(r -> listener.onResponse(Boolean.TRUE), listener::onFailure)
        );
    }

    private static IndexRequest createIndexRequest(String docId, String indexName, ToXContentObject body, boolean allowOverwriting) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            var request = new IndexRequest(indexName);
            XContentBuilder source = body.toXContent(builder, ToXContent.EMPTY_PARAMS);
            var operation = allowOverwriting ? DocWriteRequest.OpType.INDEX : DocWriteRequest.OpType.CREATE;

            return request.opType(operation).id(docId).source(source);
        } catch (IOException ex) {
            throw new ElasticsearchException(format("Unexpected serialization exception for index [%s] doc [%s]", indexName, docId), ex);
        }
    }

    private QueryBuilder documentIdQuery(String modelId) {
        return QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(Model.documentId(modelId)));
    }
}
