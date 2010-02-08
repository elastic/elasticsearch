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

package org.elasticsearch.http.action.admin.indices.mapping.create;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.http.*;
import org.elasticsearch.http.action.support.HttpActions;
import org.elasticsearch.http.action.support.HttpJsonBuilder;
import org.elasticsearch.index.mapper.InvalidTypeNameException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.*;
import static org.elasticsearch.http.HttpResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpCreateMappingAction extends BaseHttpServerHandler {

    @Inject public HttpCreateMappingAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.PUT, "/{index}/_mapping", this);
        httpService.registerHandler(HttpRequest.Method.PUT, "/{index}/{type}/_mapping", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        String[] indices = HttpActions.splitIndices(request.param("index"));
        String mappingType = request.param("type");
        String mappingSource = request.contentAsString();
        client.admin().indices().execCreateMapping(new CreateMappingRequest(indices, mappingType, mappingSource), new ActionListener<CreateMappingResponse>() {
            @Override public void onResponse(CreateMappingResponse result) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject()
                            .field("ok", true)
                            .endObject();
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    Throwable t = unwrapCause(e);
                    if (t instanceof IndexMissingException || t instanceof InvalidTypeNameException) {
                        channel.sendResponse(new JsonHttpResponse(request, BAD_REQUEST, JsonBuilder.cached().startObject().field("error", t.getMessage()).endObject()));
                    } else {
                        channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                    }
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}