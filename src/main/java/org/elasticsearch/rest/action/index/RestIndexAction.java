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

import org.elasticsearch.action.ActionRequestValidationException;
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
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestIndexAction extends BaseActionRequestRestHandler<IndexRequest> {

    @Inject
    public RestIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/{index}/{type}", this); // auto id creation
        controller.registerHandler(PUT, "/{index}/{type}/{id}", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}", this);
        CreateHandler createHandler = new CreateHandler(settings, client);
        controller.registerHandler(PUT, "/{index}/{type}/{id}/_create", createHandler);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_create", createHandler);
    }

    private RestIndexAction(Settings settings, Client client) {
        super(settings, client);
    }

    final class CreateHandler extends RestIndexAction {
        public CreateHandler(Settings settings, Client client) {
            super(settings, client);
        }

        @Override
        protected IndexRequest newRequest(RestRequest request) {
            return super.newRequest(request).opType(IndexRequest.OpType.CREATE);
        }
    }

    @Override
    protected IndexRequest newRequest(RestRequest request) {
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
        String sOpType = request.param("op_type");
        if (sOpType != null) {
            if ("index".equals(sOpType)) {
                indexRequest.opType(IndexRequest.OpType.INDEX);
            } else if ("create".equals(sOpType)) {
                indexRequest.opType(IndexRequest.OpType.CREATE);
            } else {
                ActionRequestValidationException validationException = new ActionRequestValidationException();
                validationException.addValidationError("opType [" + sOpType + "] not allowed, either [index] or [create] are allowed");
                throw validationException;
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
        return indexRequest;
    }

    @Override
    public void doHandleRequest(final RestRequest restRequest, final RestChannel channel, IndexRequest request) {
        client.index(request, new RestBuilderListener<IndexResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexResponse response, XContentBuilder builder) throws Exception {
                builder.startObject()
                        .field(Fields._INDEX, response.getIndex())
                        .field(Fields._TYPE, response.getType())
                        .field(Fields._ID, response.getId())
                        .field(Fields._VERSION, response.getVersion())
                        .field(Fields.CREATED, response.isCreated());
                builder.endObject();
                RestStatus status = OK;
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
