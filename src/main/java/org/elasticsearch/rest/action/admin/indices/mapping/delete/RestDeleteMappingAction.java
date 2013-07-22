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

package org.elasticsearch.rest.action.admin.indices.mapping.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.client.Requests.deleteMappingRequest;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.splitIndices;

/**
 *
 */
public class RestDeleteMappingAction extends BaseRestHandler {

    @Inject
    public RestDeleteMappingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(DELETE, "/{index}/{type}/_mapping", this);
        controller.registerHandler(DELETE, "/{index}/{type}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        DeleteMappingRequest deleteMappingRequest = deleteMappingRequest(splitIndices(request.param("index")));
        deleteMappingRequest.listenerThreaded(false);
        deleteMappingRequest.type(request.param("type"));
        deleteMappingRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteMappingRequest.masterNodeTimeout()));
        client.admin().indices().deleteMapping(deleteMappingRequest, new ActionListener<DeleteMappingResponse>() {
            @Override
            public void onResponse(DeleteMappingResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject()
                            .field("ok", true);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (IOException e) {
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
}
