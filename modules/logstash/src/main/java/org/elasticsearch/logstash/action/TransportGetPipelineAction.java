/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.logstash.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransportGetPipelineAction extends HandledTransportAction<GetPipelineRequest, GetPipelineResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetPipelineAction.class);
    private final Client client;

    @Inject
    public TransportGetPipelineAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetPipelineAction.NAME, transportService, actionFilters, GetPipelineRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, GetPipelineRequest request, ActionListener<GetPipelineResponse> listener) {
        if (request.ids().isEmpty()) {
            client.prepareSearch(".logstash")
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
                    final Map<String, BytesReference> pipelineSources = new HashMap<>(numHits);
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
                }, listener::onFailure));
        } else if (request.ids().size() == 1) {
            client.prepareGet(".logstash", request.ids().get(0)).setFetchSource(true).execute(ActionListener.wrap(response -> {
                if (response.isExists()) {
                    listener.onResponse(new GetPipelineResponse(Map.of(response.getId(), response.getSourceAsBytesRef())));
                } else {
                    listener.onResponse(new GetPipelineResponse(Map.of()));
                }
            }, listener::onFailure));
        } else {
            client.prepareMultiGet()
                .addIds(".logstash", request.ids())
                .execute(
                    ActionListener.wrap(
                        mGetResponse -> listener.onResponse(
                            new GetPipelineResponse(
                                Arrays.stream(mGetResponse.getResponses())
                                    .filter(itemResponse -> itemResponse.isFailed() == false)
                                    .filter(itemResponse -> itemResponse.getResponse().isExists())
                                    .map(MultiGetItemResponse::getResponse)
                                    .collect(Collectors.toMap(GetResponse::getId, GetResponse::getSourceAsBytesRef))
                            )
                        ),
                        listener::onFailure
                    )
                );
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
}
