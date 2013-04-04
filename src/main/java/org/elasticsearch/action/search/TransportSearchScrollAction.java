/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.type.ParsedScrollId;
import org.elasticsearch.action.search.type.TransportSearchScrollQueryAndFetchAction;
import org.elasticsearch.action.search.type.TransportSearchScrollQueryThenFetchAction;
import org.elasticsearch.action.search.type.TransportSearchScrollScanAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.action.search.type.ParsedScrollId.*;
import static org.elasticsearch.action.search.type.TransportSearchHelper.parseScrollId;

/**
 *
 */
public class TransportSearchScrollAction extends TransportAction<SearchScrollRequest, SearchResponse> {

    private final TransportSearchScrollQueryThenFetchAction queryThenFetchAction;

    private final TransportSearchScrollQueryAndFetchAction queryAndFetchAction;

    private final TransportSearchScrollScanAction scanAction;

    @Inject
    public TransportSearchScrollAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                       TransportSearchScrollQueryThenFetchAction queryThenFetchAction,
                                       TransportSearchScrollQueryAndFetchAction queryAndFetchAction,
                                       TransportSearchScrollScanAction scanAction) {
        super(settings, threadPool);
        this.queryThenFetchAction = queryThenFetchAction;
        this.queryAndFetchAction = queryAndFetchAction;
        this.scanAction = scanAction;

        transportService.registerHandler(SearchScrollAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        try {
            ParsedScrollId scrollId = parseScrollId(request.scrollId());
            if (scrollId.getType().equals(QUERY_THEN_FETCH_TYPE)) {
                queryThenFetchAction.execute(request, scrollId, listener);
            } else if (scrollId.getType().equals(QUERY_AND_FETCH_TYPE)) {
                queryAndFetchAction.execute(request, scrollId, listener);
            } else if (scrollId.getType().equals(SCAN)) {
                scanAction.execute(request, scrollId, listener);
            } else {
                throw new ElasticSearchIllegalArgumentException("Scroll id type [" + scrollId.getType() + "] unrecognized");
            }
        } catch (Throwable e) {
            listener.onFailure(e);
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<SearchScrollRequest> {

        @Override
        public SearchScrollRequest newInstance() {
            return new SearchScrollRequest();
        }

        @Override
        public void messageReceived(SearchScrollRequest request, final TransportChannel channel) throws Exception {
            // no need for a threaded listener
            request.listenerThreaded(false);
            execute(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for search", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
