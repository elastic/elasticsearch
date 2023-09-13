/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.Model;

import java.io.IOException;
import java.util.Map;

public class ModelRegistry {
    public record ModelConfigMap(Map<String, Object> config) {}

    private final OriginSettingClient client;

    public ModelRegistry(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    public void getUnparsedModelMap(String modelId, ActionListener<ModelConfigMap> listener) {
        ActionListener<SearchResponse> searchListener = ActionListener.wrap(searchResponse -> {
            if (searchResponse.getHits().getHits().length == 0) {
                listener.onFailure(new ResourceNotFoundException("Model not found [{}]", modelId));
                return;
            }

            var hits = searchResponse.getHits().getHits();
            assert hits.length == 1;
            listener.onResponse(new ModelConfigMap(hits[0].getSourceAsMap()));

        }, listener::onFailure);

        QueryBuilder queryBuilder = documentIdQuery(modelId);
        SearchRequest modelSearch = client.prepareSearch(InferenceIndex.INDEX_PATTERN).setQuery(queryBuilder).setSize(1).request();

        client.search(modelSearch, searchListener);
    }

    public void storeModel(Model model, ActionListener<Boolean> listener) {
        IndexRequest request = createIndexRequest(Model.documentId(model.getModelId()), InferenceIndex.INDEX_NAME, model, false);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        client.index(request, ActionListener.wrap(indexResponse -> listener.onResponse(true), e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                listener.onFailure(new ResourceAlreadyExistsException("Inference model [{}] already exists", model.getModelId()));
            } else {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Failed to store inference model [{}]",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        e,
                        model.getModelId()
                    )
                );
            }
        }));
    }

    public void deleteModel(String modelId, ActionListener<Boolean> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest().setAbortOnVersionConflict(false);
        request.indices(InferenceIndex.INDEX_PATTERN);
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
            throw new ElasticsearchException("Unexpected serialization exception for [" + docId + "]", ex);
        }
    }

    private QueryBuilder documentIdQuery(String modelId) {
        return QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(Model.documentId(modelId)));
    }
}
