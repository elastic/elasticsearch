/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.SemanticSearchAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;

import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportSemanticSearchAction extends HandledTransportAction<SemanticSearchAction.Request, SemanticSearchAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportSemanticSearchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(SemanticSearchAction.NAME, transportService, actionFilters, SemanticSearchAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, SemanticSearchAction.Request request, ActionListener<SemanticSearchAction.Response> listener) {

        var parentTask = new TaskId(clusterService.localNode().getId(), task.getId());
        var originSettingClient = new OriginSettingClient(client, ML_ORIGIN);

        long startMs = System.currentTimeMillis();
        // call inference as ML_ORIGIN
        originSettingClient.execute(
            InferModelAction.INSTANCE,
            toInferenceRequest(request, parentTask),
            ActionListener.wrap(inferenceResults -> {
                // Expect 1 result
                assert inferenceResults.getInferenceResults().size() == 1;
                if (inferenceResults.getInferenceResults().get(0)instanceof TextEmbeddingResults textEmbeddingResults) {

                    var searchRequestBuilder = buildSearch(client, textEmbeddingResults, request);
                    searchRequestBuilder.request().setParentTask(parentTask);

                    // execute search using the callers permissions
                    searchRequestBuilder.execute(ActionListener.wrap(searchResponse -> {
                        listener.onResponse(
                            new SemanticSearchAction.Response(
                                searchResponse.getTook(),
                                TimeValue.timeValueMillis(System.currentTimeMillis() - startMs),
                                searchResponse
                            )
                        );
                    }, listener::onFailure));
                } else {
                    listener.onFailure(
                        new IllegalArgumentException(
                            "model ["
                                + request.getModelId()
                                + "] must be a text_embedding model; provided ["
                                + inferenceResults.getInferenceResults().get(0).getWriteableName()
                                + "]"
                        )
                    );
                }
            }, listener::onFailure)
        );
    }

    private SearchRequestBuilder buildSearch(Client client, TextEmbeddingResults inferenceResults, SemanticSearchAction.Request request) {
        var searchBuilder = client.prepareSearch();
        searchBuilder.setIndices(request.indices());
        searchBuilder.setIndicesOptions(request.indicesOptions());
        if (request.getRouting() != null) {
            searchBuilder.setRouting(request.getRouting());
        }

        var knnSearchBuilder = request.getKnnQueryOptions().toKnnSearchBuilder(inferenceResults.getInferenceAsFloat());

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.trackTotalHitsUpTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        sourceBuilder.knnSearch(knnSearchBuilder);
        if (request.getSize() != -1) {
            sourceBuilder.size(request.getSize());
        }
        if (request.getQuery() != null) {
            sourceBuilder.query(request.getQuery());
        }
        if (request.getFetchSource() != null) {
            sourceBuilder.fetchSource(request.getFetchSource());
        }
        if (request.getFields() != null) {
            for (FieldAndFormat field : request.getFields()) {
                sourceBuilder.fetchField(field);
            }
        }
        if (request.getDocValueFields() != null) {
            for (FieldAndFormat field : request.getDocValueFields()) {
                sourceBuilder.docValueField(field.field, field.format);
            }
        }
        if (request.getStoredFields() != null) {
            sourceBuilder.storedFields(request.getStoredFields());
        }

        searchBuilder.setSource(sourceBuilder);
        return searchBuilder;
    }

    private InferModelAction.Request toInferenceRequest(SemanticSearchAction.Request request, TaskId parentTask) {

        var configUpdate = request.getEmbeddingConfig();
        if (configUpdate == null) {
            configUpdate = TextEmbeddingConfigUpdate.EMPTY_INSTANCE;
        }
        var inferenceRequest = InferModelAction.Request.forTextInput(request.getModelId(), configUpdate, List.of(request.getModelText()));
        inferenceRequest.setInferenceTimeout(request.getInferenceTimeout());
        inferenceRequest.setParentTask(parentTask);
        return inferenceRequest;
    }
}
