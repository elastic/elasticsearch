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

package org.elasticsearch.percolator;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportMultiPercolateAction extends HandledTransportAction<MultiPercolateRequest, MultiPercolateResponse> {

    private final Client client;
    private final ParseFieldMatcher parseFieldMatcher;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggParsers;

    @Inject
    public TransportMultiPercolateAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, Client client, IndicesQueriesRegistry queryRegistry,
                                         AggregatorParsers aggParsers) {
        super(settings, MultiPercolateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, MultiPercolateRequest::new);
        this.client = client;
        this.aggParsers = aggParsers;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.queryRegistry = queryRegistry;
    }

    @Override
    protected void doExecute(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener) {
        List<Tuple<Integer, GetRequest>> getRequests = new ArrayList<>();
        for (int i = 0; i < request.requests().size(); i++) {
            GetRequest getRequest = request.requests().get(i).getRequest();
            if (getRequest != null) {
                getRequests.add(new Tuple<>(i, getRequest));
            }
        }
        if (getRequests.isEmpty()) {
            innerDoExecute(request, listener, Collections.emptyMap(), new HashMap<>());
        } else {
            MultiGetRequest multiGetRequest = new MultiGetRequest();
            for (Tuple<Integer, GetRequest> tuple : getRequests) {
                GetRequest getRequest = tuple.v2();
                multiGetRequest.add(new MultiGetRequest.Item(getRequest.index(), getRequest.type(), getRequest.id()));
            }
            client.multiGet(multiGetRequest, new ActionListener<MultiGetResponse>() {
                @Override
                public void onResponse(MultiGetResponse response) {
                    Map<Integer, BytesReference> getResponseSources = new HashMap<>(response.getResponses().length);
                    Map<Integer, MultiPercolateResponse.Item> preFailures = new HashMap<>();
                    for (int i = 0; i < response.getResponses().length; i++) {
                        MultiGetItemResponse itemResponse = response.getResponses()[i];
                        int originalSlot = getRequests.get(i).v1();
                        if (itemResponse.isFailed()) {
                            preFailures.put(originalSlot, new MultiPercolateResponse.Item(itemResponse.getFailure().getFailure()));
                        } else {
                            if (itemResponse.getResponse().isExists()) {
                                getResponseSources.put(originalSlot, itemResponse.getResponse().getSourceAsBytesRef());
                            } else {
                                GetRequest getRequest = getRequests.get(i).v2();
                                preFailures.put(originalSlot, new MultiPercolateResponse.Item(new ResourceNotFoundException("percolate document [{}/{}/{}] doesn't exist", getRequest.index(), getRequest.type(), getRequest.id())));
                            }
                        }
                    }
                    innerDoExecute(request, listener, getResponseSources, preFailures);
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    private void innerDoExecute(MultiPercolateRequest request, ActionListener<MultiPercolateResponse> listener, Map<Integer, BytesReference> getResponseSources, Map<Integer, MultiPercolateResponse.Item> preFailures) {
        try {
            MultiSearchRequest multiSearchRequest = createMultiSearchRequest(request, getResponseSources, preFailures);
            if (multiSearchRequest.requests().isEmpty()) {
                // we may failed to turn all percolate requests into search requests,
                // in that case just return the response...
                listener.onResponse(
                        createMultiPercolateResponse(new MultiSearchResponse(new MultiSearchResponse.Item[0]), request, preFailures)
                );
            } else {
                client.multiSearch(multiSearchRequest, new ActionListener<MultiSearchResponse>() {
                    @Override
                    public void onResponse(MultiSearchResponse response) {
                        try {
                            listener.onResponse(createMultiPercolateResponse(response, request, preFailures));
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private MultiSearchRequest createMultiSearchRequest(MultiPercolateRequest multiPercolateRequest, Map<Integer, BytesReference> getResponseSources, Map<Integer, MultiPercolateResponse.Item> preFailures) throws IOException {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.indicesOptions(multiPercolateRequest.indicesOptions());

        for (int i  = 0; i < multiPercolateRequest.requests().size(); i++) {
            if (preFailures.keySet().contains(i)) {
                continue;
            }

            PercolateRequest percolateRequest = multiPercolateRequest.requests().get(i);
            BytesReference docSource = getResponseSources.get(i);
            try {
                SearchRequest searchRequest = TransportPercolateAction.createSearchRequest(
                        percolateRequest, docSource, queryRegistry, aggParsers, parseFieldMatcher
                );
                multiSearchRequest.add(searchRequest);
            } catch (Exception e) {
                preFailures.put(i, new MultiPercolateResponse.Item(e));
            }
        }

        return multiSearchRequest;
    }

    private MultiPercolateResponse createMultiPercolateResponse(MultiSearchResponse multiSearchResponse, MultiPercolateRequest request, Map<Integer, MultiPercolateResponse.Item> preFailures) {
        int searchResponseIndex = 0;
        MultiPercolateResponse.Item[] percolateItems = new MultiPercolateResponse.Item[request.requests().size()];
        for (int i = 0; i < percolateItems.length; i++) {
            if (preFailures.keySet().contains(i)) {
                percolateItems[i] = preFailures.get(i);
            } else {
                MultiSearchResponse.Item searchItem = multiSearchResponse.getResponses()[searchResponseIndex++];
                if (searchItem.isFailure()) {
                    percolateItems[i] = new MultiPercolateResponse.Item(searchItem.getFailure());
                } else {
                    PercolateRequest percolateRequest = request.requests().get(i);
                    percolateItems[i] = new MultiPercolateResponse.Item(TransportPercolateAction.createPercolateResponse(searchItem.getResponse(), percolateRequest.onlyCount()));
                }
            }
        }
        return new MultiPercolateResponse(percolateItems);
    }

}
