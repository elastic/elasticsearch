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

package org.elasticsearch.http.action.get;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.http.*;
import org.elasticsearch.http.action.support.HttpJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.http.HttpResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpGetAction extends BaseHttpServerHandler {

    @Inject public HttpGetAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/{type}/{id}", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        // no need to have a threaded listener since we just send back a response
        getRequest.listenerThreaded(false);
        // if we have a local operation, execute it on a thread since we don't spawn
        getRequest.threadedOperation(true);
        client.execGet(getRequest, new ActionListener<GetResponse>() {
            @Override public void onResponse(GetResponse result) {
                try {
                    if (result.empty()) {
                        channel.sendResponse(new JsonHttpResponse(request, NOT_FOUND));
                    } else {
                        JsonBuilder builder = HttpJsonBuilder.cached(request);
                        builder.startObject();
                        builder.field("_index", result.index());
                        builder.field("_type", result.type());
                        builder.field("_id", result.id());
                        builder.raw(", \"_source\" : ");
                        builder.raw(result.source());
                        builder.endObject();
                        channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                    }
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