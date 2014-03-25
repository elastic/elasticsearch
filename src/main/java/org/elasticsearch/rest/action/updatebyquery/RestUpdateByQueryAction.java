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

package org.elasticsearch.rest.action.updatebyquery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.updatebyquery.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Rest handler for update by query requests.
 */
public class RestUpdateByQueryAction extends BaseRestHandler {

    @Inject
    public RestUpdateByQueryAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/{index}/_update_by_query", this);
        controller.registerHandler(POST, "/{index}/{type}/_update_by_query", this);
    }

    public void handleRequest(final RestRequest request, final RestChannel channel) {
        UpdateByQueryRequest udqRequest = new UpdateByQueryRequest(
                Strings.splitStringByCommaToArray(request.param("index")),
                Strings.splitStringByCommaToArray(request.param("type"))
        );
        udqRequest.listenerThreaded(false);
        String replicationType = request.param("replication");
        if (replicationType != null) {
            udqRequest.replicationType(ReplicationType.fromString(replicationType));
        }
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            udqRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        String responseType = request.param("response");
        if (responseType != null) {
            udqRequest.bulkResponseOptions(BulkResponseOption.fromString(responseType));
        }
        udqRequest.routing(request.param("routing"));
        String timeout = request.param("timeout");
        if (timeout != null) {
            udqRequest.timeout(TimeValue.parseTimeValue(timeout, null));
        }

        // see if we have it in the body
        if (request.hasContent()) {
            udqRequest.source(request.content(), request.contentUnsafe());
        } else if (request.hasParam("source")) {
            udqRequest.source(new BytesArray(request.param("source")), false);
        } else if (request.hasParam("q")) {
            UpdateByQuerySourceBuilder sourceBuilder = new UpdateByQuerySourceBuilder();
            sourceBuilder.script(request.param("script"));
            sourceBuilder.scriptLang(request.param("lang"));
            for (Map.Entry<String, String> entry : request.params().entrySet()) {
                if (entry.getKey().startsWith("sp_")) {
                    sourceBuilder.addScriptParam(entry.getKey().substring(3), entry.getValue());
                }
            }

            sourceBuilder.query(RestActions.parseQuerySource(request).buildAsBytes(XContentType.JSON));
            udqRequest.source(sourceBuilder);
        }

        client.updateByQuery(udqRequest, new ActionListener<UpdateByQueryResponse>() {

            public void onResponse(UpdateByQueryResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);

                    builder.startObject();
                    builder.field(Fields.OK, !response.hasFailures());
                    builder.field(Fields.TOOK, response.tookInMillis());
                    builder.field(Fields.TOTAL, response.totalHits());
                    builder.field(Fields.UPDATED, response.updated());

                    if (response.hasFailures()) {
                        builder.startObject(Fields.ERRORS);
                        builder.startArray();
                        for (String failure : response.mainFailures()) {
                            builder.field(Fields.ERROR, failure);
                        }
                        builder.endArray();
                        builder.endObject();
                    }

                    if (response.indexResponses().length != 0) {
                        builder.startArray(Fields.INDICES);
                        for (IndexUpdateByQueryResponse indexResponse : response.indexResponses()) {
                            builder.startObject();
                            builder.field(indexResponse.index());
                            builder.startObject();
                            for (Map.Entry<Integer, BulkItemResponse[]> shard : indexResponse.responsesByShard().entrySet()) {
                                builder.startObject(shard.getKey().toString());
                                if (indexResponse.failuresByShard().containsKey(shard.getKey())) {
                                    builder.field(Fields.ERROR, indexResponse.failuresByShard().get(shard.getKey()));
                                }
                                builder.startArray(Fields.ITEMS);
                                for (BulkItemResponse itemResponse : shard.getValue()) {
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
                                        builder.field(Fields.ERROR, itemResponse.getFailure().getMessage());
                                    } else {
                                        builder.field(Fields.OK, true);
                                    }
                                    builder.endObject();
                                    builder.endObject();
                                }
                                builder.endArray();
                                builder.endObject();
                            }
                            for (Map.Entry<Integer, String> shard : indexResponse.failuresByShard().entrySet()) {
                                builder.startObject(shard.getKey().toString());
                                builder.field(Fields.ERROR, shard.getValue());
                                builder.endObject();
                            }
                            builder.endObject();
                            builder.endObject();
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

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
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString ERRORS = new XContentBuilderString("errors");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString UPDATED = new XContentBuilderString("updated");
        static final XContentBuilderString ITEMS = new XContentBuilderString("items");
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
    }
}
