/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.OK;

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

    private final boolean allowExplicitIndex;

    @Inject
    public RestBulkAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(POST, "/_bulk", this);
        controller.registerHandler(PUT, "/_bulk", this);
        controller.registerHandler(POST, "/{index}/_bulk", this);
        controller.registerHandler(PUT, "/{index}/_bulk", this);
        controller.registerHandler(POST, "/{index}/{type}/_bulk", this);
        controller.registerHandler(PUT, "/{index}/{type}/_bulk", this);

        this.allowExplicitIndex = settings.getAsBoolean("rest.action.multi.allow_explicit_index", true);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        BulkRequest bulkRequest = Requests.bulkRequest();
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        String defaultRouting = request.param("routing");
        String fieldsParam = request.param("fields");
        String[] defaultFields = fieldsParam != null ? Strings.commaDelimitedListToStringArray(fieldsParam) : null;

        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            bulkRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.refresh(request.paramAsBoolean("refresh", bulkRequest.refresh()));
        bulkRequest.add(request.content(), defaultIndex, defaultType, defaultRouting, defaultFields, null, allowExplicitIndex);

        client.bulk(bulkRequest, new RestBuilderListener<BulkResponse>(channel) {
            @Override
            public RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Fields.TOOK, response.getTookInMillis());
                builder.field(Fields.ERRORS, response.hasFailures());
                builder.startArray(Fields.ITEMS);
                for (BulkItemResponse itemResponse : response) {
                    builder.startObject();
                    builder.startObject(itemResponse.getOpType());
                    builder.field(Fields._INDEX, itemResponse.getIndex());
                    builder.field(Fields._TYPE, itemResponse.getType());
                    builder.field(Fields._ID, itemResponse.getId());
                    long version = itemResponse.getVersion();
                    if (version != -1) {
                        builder.field(Fields._VERSION, itemResponse.getVersion());
                    }
                    if (itemResponse.isFailed()) {
                        builder.field(Fields.STATUS, itemResponse.getFailure().getStatus().getStatus());
                        builder.startObject(Fields.ERROR);
                        ElasticsearchException.toXContent(builder, request, itemResponse.getFailure().getCause());
                        builder.endObject();
                    } else {
                        ActionWriteResponse.ShardInfo shardInfo = itemResponse.getResponse().getShardInfo();
                        shardInfo.toXContent(builder, request);
                        if (itemResponse.getResponse() instanceof DeleteResponse) {
                            DeleteResponse deleteResponse = itemResponse.getResponse();
                            if (deleteResponse.isFound()) {
                                builder.field(Fields.STATUS, shardInfo.status().getStatus());
                            } else {
                                builder.field(Fields.STATUS, RestStatus.NOT_FOUND.getStatus());
                            }
                            builder.field(Fields.FOUND, deleteResponse.isFound());
                        } else if (itemResponse.getResponse() instanceof IndexResponse) {
                            IndexResponse indexResponse = itemResponse.getResponse();
                            if (indexResponse.isCreated()) {
                                builder.field(Fields.STATUS, RestStatus.CREATED.getStatus());
                            } else {
                                builder.field(Fields.STATUS, shardInfo.status().getStatus());
                            }
                        } else if (itemResponse.getResponse() instanceof UpdateResponse) {
                            UpdateResponse updateResponse = itemResponse.getResponse();
                            if (updateResponse.isCreated()) {
                                builder.field(Fields.STATUS, RestStatus.CREATED.getStatus());
                            } else {
                                builder.field(Fields.STATUS, shardInfo.status().getStatus());
                            }
                            if (updateResponse.getGetResult() != null) {
                                builder.startObject(Fields.GET);
                                updateResponse.getGetResult().toXContentEmbedded(builder, request);
                                builder.endObject();
                            }
                        }
                    }
                    builder.endObject();
                    builder.endObject();
                }
                builder.endArray();

                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString ITEMS = new XContentBuilderString("items");
        static final XContentBuilderString ERRORS = new XContentBuilderString("errors");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString FOUND = new XContentBuilderString("found");
        static final XContentBuilderString GET = new XContentBuilderString("get");
    }

}
