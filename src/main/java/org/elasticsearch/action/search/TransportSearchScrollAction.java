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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.type.ParsedScrollId;
import org.elasticsearch.action.search.type.TransportSearchScrollQueryAndFetchAction;
import org.elasticsearch.action.search.type.TransportSearchScrollQueryThenFetchAction;
import org.elasticsearch.action.search.type.TransportSearchScrollScanAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.action.search.type.ParsedScrollId.*;
import static org.elasticsearch.action.search.type.TransportSearchHelper.parseScrollId;

/**
 *
 */
public class TransportSearchScrollAction extends HandledTransportAction<SearchScrollRequest, SearchResponse> {

    private final TransportSearchScrollQueryThenFetchAction queryThenFetchAction;
    private final TransportSearchScrollQueryAndFetchAction queryAndFetchAction;
    private final TransportSearchScrollScanAction scanAction;

    @Inject
    public TransportSearchScrollAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                       TransportSearchScrollQueryThenFetchAction queryThenFetchAction,
                                       TransportSearchScrollQueryAndFetchAction queryAndFetchAction,
                                       TransportSearchScrollScanAction scanAction, ActionFilters actionFilters) {
        super(settings, SearchScrollAction.NAME, threadPool, transportService, actionFilters, SearchScrollRequest.class);
        this.queryThenFetchAction = queryThenFetchAction;
        this.queryAndFetchAction = queryAndFetchAction;
        this.scanAction = scanAction;
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
                throw new ElasticsearchIllegalArgumentException("Scroll id type [" + scrollId.getType() + "] unrecognized");
            }
        } catch (Throwable e) {
            listener.onFailure(e);
        }
    }
}
