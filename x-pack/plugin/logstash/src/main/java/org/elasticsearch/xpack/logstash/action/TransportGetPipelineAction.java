/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.logstash.Logstash;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.LOGSTASH_MANAGEMENT_ORIGIN;

public class TransportGetPipelineAction extends HandledTransportAction<GetPipelineRequest, GetPipelineResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetPipelineAction.class);
    private final Client client;

    @Inject
    public TransportGetPipelineAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetPipelineAction.NAME, transportService, actionFilters, GetPipelineRequest::new);
        this.client = new OriginSettingClient(client, LOGSTASH_MANAGEMENT_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, GetPipelineRequest request, ActionListener<GetPipelineResponse> listener) {
        if (request.ids().isEmpty()) {
            client.prepareSearch(Logstash.LOGSTASH_CONCRETE_INDEX_NAME)
                .setSource(
                    SearchSourceBuilder.searchSource()
                        .fetchSource(true)
                        .query(QueryBuilders.matchAllQuery())
                        .size(1000)
                        .trackTotalHits(true)
                )
                .setScroll(TimeValue.timeValueMinutes(1L))
                .execute(ActionListener.wrap(searchResponse -> {
                    final int numHits = Math.toIntExact(searchResponse.getHits().getTotalHits().value);
                    final Map<String, BytesReference> pipelineSources = Maps.newMapWithExpectedSize(numHits);
                    final Consumer<SearchResponse> clearScroll = (response) -> {
                        if (response != null && response.getScrollId() != null) {
                            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                            clearScrollRequest.addScrollId(response.getScrollId());
                            client.clearScroll(
                                clearScrollRequest,
                                ActionListener.wrap(
                                    (r) -> {},
                                    e -> logger.warn(
                                        new ParameterizedMessage("clear scroll failed for scroll id [{}]", response.getScrollId()),
                                        e
                                    )
                                )
                            );
                        }
                    };
                    handleSearchResponse(searchResponse, pipelineSources, clearScroll, listener);
                }, e -> handleFailure(e, listener)));
        } else if (request.ids().size() == 1) {
            client.prepareGet(Logstash.LOGSTASH_CONCRETE_INDEX_NAME, request.ids().get(0))
                .setFetchSource(true)
                .execute(ActionListener.wrap(response -> {
                    if (response.isExists()) {
                        listener.onResponse(new GetPipelineResponse(Map.of(response.getId(), response.getSourceAsBytesRef())));
                    } else {
                        listener.onResponse(new GetPipelineResponse(Map.of()));
                    }
                }, e -> handleFailure(e, listener)));
        } else {
            client.prepareMultiGet()
                .addIds(Logstash.LOGSTASH_CONCRETE_INDEX_NAME, request.ids())
                .execute(ActionListener.wrap(mGetResponse -> {
                    logFailures(mGetResponse);
                    listener.onResponse(
                        new GetPipelineResponse(
                            Arrays.stream(mGetResponse.getResponses())
                                .filter(itemResponse -> itemResponse.isFailed() == false)
                                .filter(itemResponse -> itemResponse.getResponse().isExists())
                                .map(MultiGetItemResponse::getResponse)
                                .collect(Collectors.toMap(GetResponse::getId, GetResponse::getSourceAsBytesRef))
                        )
                    );
                }, e -> handleFailure(e, listener)));
        }
    }

    private void handleFailure(Exception e, ActionListener<GetPipelineResponse> listener) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof IndexNotFoundException) {
            listener.onResponse(new GetPipelineResponse(Map.of()));
        } else {
            listener.onFailure(e);
        }
    }

    private void handleSearchResponse(
        SearchResponse searchResponse,
        Map<String, BytesReference> pipelineSources,
        Consumer<SearchResponse> clearScroll,
        ActionListener<GetPipelineResponse> listener
    ) {
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            pipelineSources.put(hit.getId(), hit.getSourceRef());
        }

        if (pipelineSources.size() > searchResponse.getHits().getTotalHits().value) {
            clearScroll.accept(searchResponse);
            listener.onFailure(
                new IllegalStateException(
                    "scrolling returned more hits ["
                        + pipelineSources.size()
                        + "] than expected ["
                        + searchResponse.getHits().getTotalHits().value
                        + "] so bailing out to prevent unbounded "
                        + "memory consumption."
                )
            );
        } else if (pipelineSources.size() == searchResponse.getHits().getTotalHits().value) {
            clearScroll.accept(searchResponse);
            listener.onResponse(new GetPipelineResponse(pipelineSources));
        } else {
            client.prepareSearchScroll(searchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(1L))
                .execute(
                    ActionListener.wrap(
                        searchResponse1 -> handleSearchResponse(searchResponse1, pipelineSources, clearScroll, listener),
                        listener::onFailure
                    )
                );
        }
    }

    private void logFailures(MultiGetResponse multiGetResponse) {
        List<String> ids = Arrays.stream(multiGetResponse.getResponses())
            .filter(MultiGetItemResponse::isFailed)
            .filter(itemResponse -> itemResponse.getFailure() != null)
            .map(itemResponse -> itemResponse.getFailure().getId())
            .collect(Collectors.toList());
        if (ids.isEmpty() == false) {
            logger.info("Could not retrieve logstash pipelines with ids: {}", ids);
        }
    }
}
