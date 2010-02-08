/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.search;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.search.type.TransportSearchDfsQueryAndFetchAction;
import org.elasticsearch.action.search.type.TransportSearchDfsQueryThenFetchAction;
import org.elasticsearch.action.search.type.TransportSearchQueryAndFetchAction;
import org.elasticsearch.action.search.type.TransportSearchQueryThenFetchAction;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import static org.elasticsearch.action.search.SearchType.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportSearchAction extends BaseAction<SearchRequest, SearchResponse> {

    private final TransportSearchDfsQueryThenFetchAction dfsQueryThenFetchAction;

    private final TransportSearchQueryThenFetchAction queryThenFetchAction;

    private final TransportSearchDfsQueryAndFetchAction dfsQueryAndFetchAction;

    private final TransportSearchQueryAndFetchAction queryAndFetchAction;

    @Inject public TransportSearchAction(Settings settings, TransportService transportService,
                                         TransportSearchDfsQueryThenFetchAction dfsQueryThenFetchAction,
                                         TransportSearchQueryThenFetchAction queryThenFetchAction,
                                         TransportSearchDfsQueryAndFetchAction dfsQueryAndFetchAction,
                                         TransportSearchQueryAndFetchAction queryAndFetchAction) {
        super(settings);
        this.dfsQueryThenFetchAction = dfsQueryThenFetchAction;
        this.queryThenFetchAction = queryThenFetchAction;
        this.dfsQueryAndFetchAction = dfsQueryAndFetchAction;
        this.queryAndFetchAction = queryAndFetchAction;

        transportService.registerHandler(TransportActions.SEARCH, new TransportHandler());
    }

    @Override protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
            dfsQueryThenFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.QUERY_THEN_FETCH) {
            queryThenFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.DFS_QUERY_AND_FETCH) {
            dfsQueryAndFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.QUERY_AND_FETCH) {
            queryAndFetchAction.execute(searchRequest, listener);
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<SearchRequest> {

        @Override public SearchRequest newInstance() {
            return new SearchRequest();
        }

        @Override public void messageReceived(SearchRequest request, final TransportChannel channel) throws Exception {
            // no need for a threaded listener
            request.listenerThreaded(false);
            // we don't spawn, so if we get a request with no threading, change it to single threaded
            if (request.operationThreading() == SearchOperationThreading.NO_THREADS) {
                request.operationThreading(SearchOperationThreading.SINGLE_THREAD);
            }
            execute(request, new ActionListener<SearchResponse>() {
                @Override public void onResponse(SearchResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for search", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }
}
