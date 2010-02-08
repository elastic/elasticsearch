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

package org.elasticsearch.http.action.admin.indices.status;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.status.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.http.*;
import org.elasticsearch.http.action.support.HttpJsonBuilder;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.http.HttpResponse.Status.*;
import static org.elasticsearch.http.action.support.HttpActions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpIndicesStatusAction extends BaseHttpServerHandler {

    @Inject public HttpIndicesStatusAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.GET, "/_status", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/_status", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        IndicesStatusRequest indicesStatusRequest = new IndicesStatusRequest(splitIndices(request.param("index")));
        indicesStatusRequest.listenerThreaded(false);
        client.admin().indices().execStatus(indicesStatusRequest, new ActionListener<IndicesStatusResponse>() {
            @Override public void onResponse(IndicesStatusResponse response) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject();
                    builder.field("ok", true);

                    builder.startObject("indices");
                    for (IndexStatus indexStatus : response.indices().values()) {
                        builder.startObject(indexStatus.index());

                        builder.startObject("settings");
                        for (Map.Entry<String, String> entry : indexStatus.settings().getAsMap().entrySet()) {
                            builder.startObject("setting").field("name", entry.getKey()).field("value", entry.getValue()).endObject();
                        }
                        builder.endObject();

                        builder.field("storeSize", indexStatus.storeSize().toString());
                        builder.field("storeSizeInBytes", indexStatus.storeSize().bytes());
                        builder.field("estimatedFlushableMemorySize", indexStatus.estimatedFlushableMemorySize().toString());
                        builder.field("estimatedFlushableMemorySizeInBytes", indexStatus.estimatedFlushableMemorySize().bytes());
                        builder.field("translogOperations", indexStatus.translogOperations());
                        builder.startObject("docs");
                        builder.field("numDocs", indexStatus.docs().numDocs());
                        builder.field("maxDoc", indexStatus.docs().maxDoc());
                        builder.field("deletedDocs", indexStatus.docs().deletedDocs());
                        builder.endObject();

                        builder.startObject("shards");
                        for (IndexShardStatus indexShardStatus : indexStatus) {
                            builder.startArray(Integer.toString(indexShardStatus.shardId().id()));
                            for (ShardStatus shardStatus : indexShardStatus) {
                                builder.startObject();

                                builder.startObject("routing")
                                        .field("state", shardStatus.shardRouting().state())
                                        .field("primary", shardStatus.shardRouting().primary())
                                        .field("nodeId", shardStatus.shardRouting().currentNodeId())
                                        .field("relocatingNodeId", shardStatus.shardRouting().relocatingNodeId())
                                        .field("shardId", shardStatus.shardRouting().shardId().id())
                                        .field("index", shardStatus.shardRouting().shardId().index().name())
                                        .endObject();

                                builder.field("state", shardStatus.state());
                                builder.field("storeSize", shardStatus.storeSize().toString());
                                builder.field("storeSizeInBytes", shardStatus.storeSize().bytes());
                                builder.field("estimatedFlushableMemorySize", shardStatus.estimatedFlushableMemorySize().toString());
                                builder.field("estimatedFlushableMemorySizeInBytes", shardStatus.estimatedFlushableMemorySize().bytes());
                                builder.field("translogId", shardStatus.translogId());
                                builder.field("translogOperations", shardStatus.translogOperations());
                                builder.startObject("docs");
                                builder.field("numDocs", shardStatus.docs().numDocs());
                                builder.field("maxDoc", shardStatus.docs().maxDoc());
                                builder.field("deletedDocs", shardStatus.docs().deletedDocs());
                                builder.endObject();

                                builder.endObject();
                            }
                            builder.endArray();
                        }
                        builder.endObject();

                        builder.endObject();
                    }
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
        // we don't spawn since we fork in index replication based on operation
        return false;
    }
}