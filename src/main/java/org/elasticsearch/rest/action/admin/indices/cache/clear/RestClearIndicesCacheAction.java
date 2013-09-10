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

package org.elasticsearch.rest.action.admin.indices.cache.clear;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 *
 */
public class RestClearIndicesCacheAction extends BaseRestHandler {

    @Inject
    public RestClearIndicesCacheAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_cache/clear", this);
        controller.registerHandler(POST, "/{index}/_cache/clear", this);

        controller.registerHandler(GET, "/_cache/clear", this);
        controller.registerHandler(GET, "/{index}/_cache/clear", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(Strings.splitStringByCommaToArray(request.param("index")));
        clearIndicesCacheRequest.listenerThreaded(false);
        if (request.hasParam("ignore_indices")) {
            clearIndicesCacheRequest.ignoreIndices(IgnoreIndices.fromString(request.param("ignore_indices")));
        }
        try {
            if (request.hasParam("filter")) {
                clearIndicesCacheRequest.filterCache(request.paramAsBoolean("filter", clearIndicesCacheRequest.filterCache()));
            }
            if (request.hasParam("filter_cache")) {
                clearIndicesCacheRequest.filterCache(request.paramAsBoolean("filter_cache", clearIndicesCacheRequest.filterCache()));
            }
            if (request.hasParam("field_data")) {
                clearIndicesCacheRequest.fieldDataCache(request.paramAsBoolean("field_data", clearIndicesCacheRequest.fieldDataCache()));
            }
            if (request.hasParam("fielddata")) {
                clearIndicesCacheRequest.fieldDataCache(request.paramAsBoolean("fielddata", clearIndicesCacheRequest.fieldDataCache()));
            }
            if (request.hasParam("id")) {
                clearIndicesCacheRequest.idCache(request.paramAsBoolean("id", clearIndicesCacheRequest.idCache()));
            }
            if (request.hasParam("id_cache")) {
                clearIndicesCacheRequest.idCache(request.paramAsBoolean("id_cache", clearIndicesCacheRequest.idCache()));
            }
            if (request.hasParam("recycler")) {
                clearIndicesCacheRequest.recycler(request.paramAsBoolean("recycler", clearIndicesCacheRequest.recycler()));
            }
            clearIndicesCacheRequest.fields(request.paramAsStringArray("fields", clearIndicesCacheRequest.fields()));
            clearIndicesCacheRequest.filterKeys(request.paramAsStringArray("filter_keys", clearIndicesCacheRequest.filterKeys()));

            BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operationThreading"), BroadcastOperationThreading.THREAD_PER_SHARD);
            if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
                // since we don't spawn, don't allow no_threads, but change it to a single thread
                operationThreading = BroadcastOperationThreading.THREAD_PER_SHARD;
            }
            clearIndicesCacheRequest.operationThreading(operationThreading);
        } catch (Exception e) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        client.admin().indices().clearCache(clearIndicesCacheRequest, new ActionListener<ClearIndicesCacheResponse>() {
            @Override
            public void onResponse(ClearIndicesCacheResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("ok", true);

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
