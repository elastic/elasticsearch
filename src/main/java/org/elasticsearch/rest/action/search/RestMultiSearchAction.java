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
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 */
public class RestMultiSearchAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    @Inject
    public RestMultiSearchAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(GET, "/_msearch", this);
        controller.registerHandler(POST, "/_msearch", this);
        controller.registerHandler(GET, "/{index}/_msearch", this);
        controller.registerHandler(POST, "/{index}/_msearch", this);
        controller.registerHandler(GET, "/{index}/{type}/_msearch", this);
        controller.registerHandler(POST, "/{index}/{type}/_msearch", this);

        this.allowExplicitIndex = settings.getAsBoolean("rest.action.multi.allow_explicit_index", true);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.listenerThreaded(false);

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        IgnoreIndices ignoreIndices = null;
        if (request.hasParam("ignore_indices")) {
            ignoreIndices = IgnoreIndices.fromString(request.param("ignore_indices"));
        }

        try {
            multiSearchRequest.add(request.content(), request.contentUnsafe(), indices, types, request.param("search_type"), request.param("routing"), ignoreIndices, allowExplicitIndex);
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.multiSearch(multiSearchRequest, new ActionListener<MultiSearchResponse>() {
            @Override
            public void onResponse(MultiSearchResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
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
