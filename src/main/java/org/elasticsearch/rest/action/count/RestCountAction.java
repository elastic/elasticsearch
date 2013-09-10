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

package org.elasticsearch.rest.action.count;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.action.count.CountRequest.DEFAULT_MIN_SCORE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 *
 */
public class RestCountAction extends BaseRestHandler {

    @Inject
    public RestCountAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_count", this);
        controller.registerHandler(GET, "/_count", this);
        controller.registerHandler(POST, "/{index}/_count", this);
        controller.registerHandler(GET, "/{index}/_count", this);
        controller.registerHandler(POST, "/{index}/{type}/_count", this);
        controller.registerHandler(GET, "/{index}/{type}/_count", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        CountRequest countRequest = new CountRequest(Strings.splitStringByCommaToArray(request.param("index")));
        if (request.hasParam("ignore_indices")) {
            countRequest.ignoreIndices(IgnoreIndices.fromString(request.param("ignore_indices")));
        }
        countRequest.listenerThreaded(false);
        try {
            BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operation_threading"), BroadcastOperationThreading.THREAD_PER_SHARD);
            if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
                // since we don't spawn, don't allow no_threads, but change it to a single thread
                operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
            }
            countRequest.operationThreading(operationThreading);
            if (request.hasContent()) {
                countRequest.query(request.content(), request.contentUnsafe());
            } else {
                String source = request.param("source");
                if (source != null) {
                    countRequest.query(source);
                } else {
                    BytesReference querySource = RestActions.parseQuerySource(request);
                    if (querySource != null) {
                        countRequest.query(querySource, false);
                    }
                }
            }
            countRequest.routing(request.param("routing"));
            countRequest.minScore(request.paramAsFloat("min_score", DEFAULT_MIN_SCORE));
            countRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
            countRequest.preference(request.param("preference"));
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.count(countRequest, new ActionListener<CountResponse>() {
            @Override
            public void onResponse(CountResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("count", response.getCount());

                    buildBroadcastShardsHeader(builder, response);

                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable e) {
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
