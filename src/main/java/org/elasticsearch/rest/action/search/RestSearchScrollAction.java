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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.Scroll;

import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 *
 */
public class RestSearchScrollAction extends BaseRestHandler {

    @Inject
    public RestSearchScrollAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(GET, "/_search/scroll", this);
        controller.registerHandler(POST, "/_search/scroll", this);
        controller.registerHandler(GET, "/_search/scroll/{scroll_id}", this);
        controller.registerHandler(POST, "/_search/scroll/{scroll_id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String scrollId = request.param("scroll_id");
        if (scrollId == null && request.hasContent()) {
            scrollId = request.content().toUtf8();
        }
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
        searchScrollRequest.listenerThreaded(false);
        try {
            String scroll = request.param("scroll");
            if (scroll != null) {
                searchScrollRequest.scroll(new Scroll(parseTimeValue(scroll, null)));
            }
            SearchOperationThreading operationThreading = SearchOperationThreading.fromString(request.param("operation_threading"), null);
            if (operationThreading != null) {
                if (operationThreading == SearchOperationThreading.NO_THREADS) {
                    // since we don't spawn, don't allow no_threads, but change it to a single thread
                    operationThreading = SearchOperationThreading.SINGLE_THREAD;
                }
                searchScrollRequest.operationThreading(operationThreading);
            }
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.searchScroll(searchScrollRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, response.status(), builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
