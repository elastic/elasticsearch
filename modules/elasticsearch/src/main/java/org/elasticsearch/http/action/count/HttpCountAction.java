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

package org.elasticsearch.http.action.count;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.http.*;
import org.elasticsearch.http.action.support.HttpActions;
import org.elasticsearch.http.action.support.HttpJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.action.count.CountRequest.*;
import static org.elasticsearch.http.HttpResponse.Status.*;
import static org.elasticsearch.http.action.support.HttpActions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpCountAction extends BaseHttpServerHandler {

    @Inject public HttpCountAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/_count", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/_count", this);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/{type}/_count", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/{type}/_count", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        CountRequest countRequest = new CountRequest(HttpActions.splitIndices(request.param("index")));
        // we just send back a response, no need to fork a listener
        countRequest.listenerThreaded(false);
        try {
            BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operationThreading"), BroadcastOperationThreading.SINGLE_THREAD);
            if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
                // since we don't spawn, don't allow no_threads, but change it to a single thread
                operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
            }
            countRequest.operationThreading(operationThreading);
            countRequest.querySource(HttpActions.parseQuerySource(request));
            countRequest.queryParserName(request.param("queryParserName"));
            countRequest.queryHint(request.param("queryHint"));
            countRequest.minScore(HttpActions.paramAsFloat(request.param("minScore"), DEFAULT_MIN_SCORE));
            String typesParam = request.param("type");
            if (typesParam != null) {
                countRequest.types(splitTypes(typesParam));
            }
        } catch (Exception e) {
            try {
                channel.sendResponse(new JsonHttpResponse(request, BAD_REQUEST, JsonBuilder.cached().startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.execCount(countRequest, new ActionListener<CountResponse>() {
            @Override public void onResponse(CountResponse response) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject();
                    builder.field("count", response.count());

                    builder.startObject("_shards");
                    builder.field("total", response.totalShards());
                    builder.field("successful", response.successfulShards());
                    builder.field("failed", response.failedShards());
                    builder.endObject();

                    builder.endObject();
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override public boolean spawn() {
        return false;
    }
}