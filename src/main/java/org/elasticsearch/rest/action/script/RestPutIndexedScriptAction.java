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
package org.elasticsearch.rest.action.script;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.*;

/**
 *
 */
public class RestPutIndexedScriptAction extends BaseRestHandler {

    private ScriptService scriptService = null;

    @Inject
    public RestPutIndexedScriptAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        //controller.registerHandler(GET, "/template", this);
        controller.registerHandler(POST, "/_search/script/{lang}/{id}", this);
        controller.registerHandler(PUT, "/_search/script/{lang}/{id}", this);

        controller.registerHandler(PUT, "/_search/script/{lang}/{id}/_create", new CreateHandler());
        controller.registerHandler(POST, "/_search/script/{lang}/{id}/_create", new CreateHandler());
    }

    final class CreateHandler implements RestHandler {
        @Override
        public void handleRequest(RestRequest request, RestChannel channel) {
            request.params().put("op_type", "create");
            RestPutIndexedScriptAction.this.handleRequest(request, channel);
        }
    }


    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        IndexRequest indexRequest = new IndexRequest(ScriptService.SCRIPT_INDEX, request.param("lang"), request.param("id"));
        indexRequest.listenerThreaded(false);
        indexRequest.operationThreaded(true);
        indexRequest.refresh(true); //Always refresh after indexing a script

        indexRequest.source(request.content(), request.contentUnsafe());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
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

        client.index(indexRequest, new RestBuilderListener<IndexResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexResponse response, XContentBuilder builder) throws Exception {
                builder.startObject()
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
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString CREATED = new XContentBuilderString("created");
    }

}
