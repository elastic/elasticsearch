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

package org.elasticsearch.rest.action.index;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.*;

/**
 *
 */
public class RestIndexAction extends BaseRestHandler {

    @Inject
    public RestIndexAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/{index}/{type}", this); // auto id creation
        controller.registerHandler(PUT, "/{index}/{type}/{id}", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}", this);
        CreateHandler createHandler = new CreateHandler(settings, controller, client);
        controller.registerHandler(PUT, "/{index}/{type}/{id}/_create", createHandler);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_create", createHandler);
    }

    final class CreateHandler extends BaseRestHandler {
        protected CreateHandler(Settings settings, RestController controller, Client client) {
            super(settings, controller, client);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, final Client client) {
            request.params().put("op_type", "create");
            RestIndexAction.this.handleRequest(request, channel, client);
        }
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        IndexRequest indexRequest = new IndexRequest(request.param("index"), request.param("type"), request.param("id"));
        indexRequest.listenerThreaded(false);
        indexRequest.operationThreaded(true);
        indexRequest.routing(request.param("routing"));
        indexRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
        indexRequest.timestamp(request.param("timestamp"));
        if (request.hasParam("ttl")) {
            indexRequest.ttl(request.paramAsTime("ttl", null).millis());
        }
        indexRequest.source(request.content());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
        indexRequest.refresh(request.paramAsBoolean("refresh", indexRequest.refresh()));
        indexRequest.version(RestActions.parseVersion(request));
        indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
        String sOpType = request.param("op_type");
        if (sOpType != null) {
            try {
                indexRequest.opType(IndexRequest.OpType.fromString(sOpType));
            } catch (ElasticsearchIllegalArgumentException eia){
                try {
                    XContentBuilder builder = channel.newBuilder();
                    channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", eia.getMessage()).endObject()));
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                    return;
                }
            }
        }
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            indexRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        client.index(indexRequest, new RestBuilderListener<IndexResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                ActionWriteResponse.ShardInfo shardInfo = response.getShardInfo();
                builder.field(Fields._INDEX, response.getIndex())
                        .field(Fields._TYPE, response.getType())
                        .field(Fields._ID, response.getId())
                        .field(Fields._VERSION, response.getVersion());
                shardInfo.toXContent(builder, request);
                builder.field(Fields.CREATED, response.isCreated());
                builder.endObject();
                RestStatus status = shardInfo.status();
                if (response.isCreated()) {
                    status = CREATED;
                }
                return new BytesRestResponse(status, builder);
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString CREATED = new XContentBuilderString("created");
    }

}
