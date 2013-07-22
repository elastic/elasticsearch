/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.percolate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestPercolateAction extends BaseRestHandler {

    @Inject
    public RestPercolateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/_percolate", this);
        controller.registerHandler(POST, "/{index}/{type}/_percolate", this);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_percolate", new RestPercolatingExistingDocumentAction());
    }

    @Override
    public void handleRequest(RestRequest restRequest, RestChannel restChannel) {
        PercolateRequest percolateRequest = new PercolateRequest(restRequest.param("index"), restRequest.param("type"));
        percolateRequest.routing(restRequest.param("routing"));
        percolateRequest.preference(restRequest.param("preference"));
        percolateRequest.source(restRequest.content(), restRequest.contentUnsafe());

        executePercolateRequest(percolateRequest, restRequest, restChannel);
    }

    void executePercolateRequest(PercolateRequest percolateRequest, final RestRequest restRequest, final RestChannel restChannel) {
        // we just send a response, no need to fork
        percolateRequest.listenerThreaded(false);
        client.percolate(percolateRequest, new ActionListener<PercolateResponse>() {
            @Override
            public void onResponse(PercolateResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(restRequest);
                    builder.startObject();

                    builder.field(Fields.TOOK, response.getTookInMillis());
                    builder.startObject(Fields._SHARDS);
                    builder.field(Fields.TOTAL, response.getTotalShards());
                    builder.field(Fields.SUCCESSFUL, response.getSuccessfulShards());
                    builder.field(Fields.FAILED, response.getFailedShards());
                    if (response.getShardFailures().length > 0) {
                        builder.startArray(Fields.FAILURES);
                        for (ShardOperationFailedException shardFailure : response.getShardFailures()) {
                            builder.startObject();
                            builder.field(Fields.INDEX, shardFailure.index());
                            builder.field(Fields.SHARD, shardFailure.shardId());
                            builder.field(Fields.STATUS, shardFailure.status().getStatus());
                            builder.field(Fields.REASON, shardFailure.reason());
                            builder.endObject();
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    builder.startArray(Fields.MATCHES);
                    for (Text match : response) {
                        builder.value(match);
                    }
                    builder.endArray();

                    builder.endObject();

                    restChannel.sendResponse(new XContentRestResponse(restRequest, OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    restChannel.sendResponse(new XContentThrowableRestResponse(restRequest, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    class RestPercolatingExistingDocumentAction implements RestHandler {

        @Override
        public void handleRequest(RestRequest restRequest, RestChannel restChannel) {
            String index = restRequest.param("index");
            String type = restRequest.param("type");
            PercolateRequest percolateRequest = new PercolateRequest(index, type);
            percolateRequest.routing(restRequest.param("routing"));
            percolateRequest.preference(restRequest.param("preference"));

            PercolateSourceBuilder builder = new PercolateSourceBuilder();
            builder.percolateGet().setIndex(restRequest.param("get_index", index))
                    .setType(restRequest.param("get_type", type))
                    .setId(restRequest.param("id"))
                    .setRouting(restRequest.param("get_routing"))
                    .setPreference(restRequest.param("get_preference"));
            percolateRequest.source(builder);

            executePercolateRequest(percolateRequest, restRequest, restChannel);
        }

    }

    static final class Fields {
        static final XContentBuilderString _SHARDS = new XContentBuilderString("_shards");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString SUCCESSFUL = new XContentBuilderString("successful");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString SHARD = new XContentBuilderString("shard");
        static final XContentBuilderString REASON = new XContentBuilderString("reason");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }
}
