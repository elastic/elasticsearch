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

package org.elasticsearch.rest.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * <pre>
 * { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
 * { "create" : { "_index" : "test", "_type" : "type1", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 */
public class RestBulkAction extends BaseRestHandler {

    @Inject
    public RestBulkAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(POST, "/_bulk", this);
        controller.registerHandler(PUT, "/_bulk", this);
        controller.registerHandler(POST, "/{index}/_bulk", this);
        controller.registerHandler(PUT, "/{index}/_bulk", this);
        controller.registerHandler(POST, "/{index}/{type}/_bulk", this);
        controller.registerHandler(PUT, "/{index}/{type}/_bulk", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        BulkRequest bulkRequest = Requests.bulkRequest();
        bulkRequest.listenerThreaded(false);
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");

        String replicationType = request.param("replication");
        if (replicationType != null) {
            bulkRequest.replicationType(ReplicationType.fromString(replicationType));
        }
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            bulkRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        bulkRequest.refresh(request.paramAsBoolean("refresh", bulkRequest.refresh()));
        try {
            bulkRequest.add(request.content(), request.contentUnsafe(), defaultIndex, defaultType);
        } catch (Exception e) {
            try {
                XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse response) {
                try {
                    XContentBuilder builder = restContentBuilder(request);
                    builder.startObject();
                    builder.field(Fields.TOOK, response.tookInMillis());
                    builder.startArray(Fields.ITEMS);
                    for (BulkItemResponse itemResponse : response) {
                        builder.startObject();
                        builder.startObject(itemResponse.opType());
                        builder.field(Fields._INDEX, itemResponse.index());
                        builder.field(Fields._TYPE, itemResponse.type());
                        builder.field(Fields._ID, itemResponse.id());
                        long version = itemResponse.version();
                        if (version != -1) {
                            builder.field(Fields._VERSION, itemResponse.version());
                        }
                        if (itemResponse.failed()) {
                            builder.field(Fields.ERROR, itemResponse.failure().message());
                        } else {
                            builder.field(Fields.OK, true);
                        }
                        if (itemResponse.response() instanceof IndexResponse) {
                            IndexResponse indexResponse = itemResponse.response();
                            if (indexResponse.matches() != null) {
                                builder.startArray(Fields.MATCHES);
                                for (String match : indexResponse.matches()) {
                                    builder.value(match);
                                }
                                builder.endArray();
                            }
                        }
                        builder.endObject();
                        builder.endObject();
                    }
                    builder.endArray();

                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
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

    static final class Fields {
        static final XContentBuilderString ITEMS = new XContentBuilderString("items");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }

}
