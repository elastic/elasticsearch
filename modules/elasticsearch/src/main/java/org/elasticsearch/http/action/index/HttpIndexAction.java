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

package org.elasticsearch.http.action.index;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.http.*;
import org.elasticsearch.http.action.support.HttpJsonBuilder;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.http.HttpResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpIndexAction extends BaseHttpServerHandler {

    @Inject public HttpIndexAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/{type}", this); // auto id creation
        httpService.registerHandler(HttpRequest.Method.PUT, "/{index}/{type}/{id}", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        IndexRequest indexRequest = new IndexRequest(request.param("index"), request.param("type"), request.param("id"), request.contentAsString());
        indexRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), IndexRequest.DEFAULT_TIMEOUT));
        String sOpType = request.param("opType");
        if (sOpType != null) {
            if ("index".equals(sOpType)) {
                indexRequest.opType(IndexRequest.OpType.INDEX);
            } else if ("create".equals(sOpType)) {
                indexRequest.opType(IndexRequest.OpType.CREATE);
            } else {
                try {
                    channel.sendResponse(new JsonHttpResponse(request, BAD_REQUEST, JsonBuilder.cached().startObject().field("error", "opType [" + sOpType + "] not allowed, either [index] or [create] are allowed").endObject()));
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                    return;
                }
            }
        }
        // we just send a response, no need to fork
        indexRequest.listenerThreaded(false);
        // we don't spawn, then fork if local
        indexRequest.operationThreaded(true);
        client.execIndex(indexRequest, new ActionListener<IndexResponse>() {
            @Override public void onResponse(IndexResponse result) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject()
                            .field("ok", true)
                            .field("_index", result.index())
                            .field("_type", result.type())
                            .field("_id", result.id())
                            .endObject();
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