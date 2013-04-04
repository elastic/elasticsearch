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

package org.elasticsearch.rest.action.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.*;

/**
 *
 */
public class RestIndexAction extends BaseRestHandler {

    @Inject
    public RestIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/{index}/{type}", this); // auto id creation
        controller.registerHandler(PUT, "/{index}/{type}/{id}", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}", this);
        controller.registerHandler(PUT, "/{index}/{type}/{id}/_create", new CreateHandler());
        controller.registerHandler(POST, "/{index}/{type}/{id}/_create", new CreateHandler());
    }

    final class CreateHandler implements RestHandler {
        @Override
        public void handleRequest(RestRequest request, RestChannel channel) {
            request.params().put("op_type", "create");
            RestIndexAction.this.handleRequest(request, channel);
        }
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        IndexRequest indexRequest = new IndexRequest(request.param("index"), request.param("type"), request.param("id"));
        indexRequest.listenerThreaded(false);
        indexRequest.operationThreaded(true);
        indexRequest.routing(request.param("routing"));
        indexRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
        indexRequest.timestamp(request.param("timestamp"));
        if (request.hasParam("ttl")) {
            indexRequest.ttl(request.paramAsTime("ttl", null).millis());
        }
        indexRequest.source(request.content(), request.contentUnsafe());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
        indexRequest.refresh(request.paramAsBoolean("refresh", indexRequest.refresh()));
        indexRequest.version(RestActions.parseVersion(request));
        indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
        indexRequest.percolate(request.param("percolate", null));
        String sOpType = request.param("op_type");
        if (sOpType != null) {
            if ("index".equals(sOpType)) {
                indexRequest.opType(IndexRequest.OpType.INDEX);
            } else if ("create".equals(sOpType)) {
                indexRequest.opType(IndexRequest.OpType.CREATE);
            } else {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    channel.sendResponse(new XContentRestResponse(request, BAD_REQUEST, builder.startObject().field("error", "opType [" + sOpType + "] not allowed, either [index] or [create] are allowed").endObject()));
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                    return;
                }
            }
        }
        String replicationType = request.param("replication");
        if (replicationType != null) {
            indexRequest.replicationType(ReplicationType.fromString(replicationType));
        }
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            indexRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        client.index(indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field(Fields.OK, true)
                            .field(Fields._INDEX, response.getIndex())
                            .field(Fields._TYPE, response.getType())
                            .field(Fields._ID, response.getId())
                            .field(Fields._VERSION, response.getVersion());
                    if (response.getMatches() != null) {
                        builder.startArray(Fields.MATCHES);
                        for (String match : response.getMatches()) {
                            builder.value(match);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                    RestStatus status = OK;
                    if (response.getVersion() == 1) {
                        status = CREATED;
                    }
                    channel.sendResponse(new XContentRestResponse(request, status, builder));
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

    static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }

}
