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

package org.elasticsearch.http.action.delete;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
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
public class HttpDeleteAction extends BaseHttpServerHandler {

    @Inject public HttpDeleteAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.DELETE, "/{index}/{type}/{id}", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        DeleteRequest deleteRequest = new DeleteRequest(request.param("index"), request.param("type"), request.param("id"));
        deleteRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), DeleteRequest.DEFAULT_TIMEOUT));
        // we just send a response, no need to fork
        deleteRequest.listenerThreaded(false);
        // we don't spawn, then fork if local
        deleteRequest.operationThreaded(true);
        client.execDelete(deleteRequest, new ActionListener<DeleteResponse>() {
            @Override public void onResponse(DeleteResponse result) {
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