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

package org.elasticsearch.rest.action.admin.indices.status;

import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.status.*;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.*;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestResponse.Status.*;
import static org.elasticsearch.rest.action.support.RestActions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestIndicesStatusAction extends BaseRestHandler {

    @Inject public RestIndicesStatusAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_status", this);
        controller.registerHandler(GET, "/{index}/_status", this);
    }

    @Override public void handleRequest(final RestRequest request, final RestChannel channel) {
        IndicesStatusRequest indicesStatusRequest = new IndicesStatusRequest(splitIndices(request.param("index")));
        // we just send back a response, no need to fork a listener
        indicesStatusRequest.listenerThreaded(false);
        BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operation_threading"), BroadcastOperationThreading.SINGLE_THREAD);
        if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
            // since we don't spawn, don't allow no_threads, but change it to a single thread
            operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
        }
        indicesStatusRequest.operationThreading(operationThreading);
        client.admin().indices().status(indicesStatusRequest, new ActionListener<IndicesStatusResponse>() {
            @Override public void onResponse(IndicesStatusResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field("ok", true);

                    buildBroadcastShardsHeader(builder, response);

                    builder.startObject("indices");
                    for (IndexStatus indexStatus : response.indices().values()) {
                        builder.startObject(indexStatus.index());

                        builder.array("aliases", indexStatus.settings().getAsArray("index.aliases"));

                        builder.startObject("settings");
                        for (Map.Entry<String, String> entry : indexStatus.settings().getAsMap().entrySet()) {
                            builder.field(entry.getKey(), entry.getValue());
                        }
                        builder.endObject();

                        if (indexStatus.storeSize() == null) {
                            builder.nullField("store_size");
                            builder.nullField("store_size_in_bytes");
                        } else {
                            builder.field("store_size", indexStatus.storeSize().toString());
                            builder.field("store_size_in_bytes", indexStatus.storeSize().bytes());
                        }
                        if (indexStatus.estimatedFlushableMemorySize() == null) {
                            builder.nullField("estimated_flushable_memory_size");
                            builder.nullField("estimated_flushable_memory_size_in_bytes");
                        } else {
                            builder.field("estimated_flushable_memory_size", indexStatus.estimatedFlushableMemorySize().toString());
                            builder.field("estimated_flushable_memory_size_in_bytes", indexStatus.estimatedFlushableMemorySize().bytes());
                        }
                        builder.field("translog_operations", indexStatus.translogOperations());

                        builder.startObject("docs");
                        builder.field("num_docs", indexStatus.docs().numDocs());
                        builder.field("max_doc", indexStatus.docs().maxDoc());
                        builder.field("deleted_docs", indexStatus.docs().deletedDocs());
                        builder.endObject();

                        builder.startObject("shards");
                        for (IndexShardStatus indexShardStatus : indexStatus) {
                            builder.startArray(Integer.toString(indexShardStatus.shardId().id()));
                            for (ShardStatus shardStatus : indexShardStatus) {
                                builder.startObject();

                                builder.startObject("routing")
                                        .field("state", shardStatus.shardRouting().state())
                                        .field("primary", shardStatus.shardRouting().primary())
                                        .field("node", shardStatus.shardRouting().currentNodeId())
                                        .field("relocating_node", shardStatus.shardRouting().relocatingNodeId())
                                        .field("shard", shardStatus.shardRouting().shardId().id())
                                        .field("index", shardStatus.shardRouting().shardId().index().name())
                                        .endObject();

                                builder.field("state", shardStatus.state());
                                if (shardStatus.storeSize() == null) {
                                    builder.nullField("store_size");
                                    builder.nullField("store_size_in_bytes");
                                } else {
                                    builder.field("store_size", shardStatus.storeSize().toString());
                                    builder.field("store_size_in_bytes", shardStatus.storeSize().bytes());
                                }
                                if (shardStatus.estimatedFlushableMemorySize() == null) {
                                    builder.nullField("estimated_flushable_memory_size");
                                    builder.nullField("estimated_flushable_memory_size_in_bytes");
                                } else {
                                    builder.field("estimated_flushable_memory_size", shardStatus.estimatedFlushableMemorySize().toString());
                                    builder.field("estimated_flushable_memory_size_in_bytes", shardStatus.estimatedFlushableMemorySize().bytes());
                                }
                                builder.field("translog_id", shardStatus.translogId());
                                builder.field("translog_operations", shardStatus.translogOperations());

                                builder.startObject("docs");
                                builder.field("num_docs", shardStatus.docs().numDocs());
                                builder.field("max_doc", shardStatus.docs().maxDoc());
                                builder.field("deleted_docs", shardStatus.docs().deletedDocs());
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
                    channel.sendResponse(new JsonRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}