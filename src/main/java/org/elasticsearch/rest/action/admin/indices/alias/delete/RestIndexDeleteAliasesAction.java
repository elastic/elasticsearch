/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.rest.action.admin.indices.alias.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestIndexDeleteAliasesAction extends BaseRestHandler {

    @Inject
    public RestIndexDeleteAliasesAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(DELETE, "/{index}/_alias/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String index = request.param("index");
        String alias = request.param("name");
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.timeout(request.paramAsTime("timeout", timeValueSeconds(10)));
        indicesAliasesRequest.removeAlias(index, alias);
        indicesAliasesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", indicesAliasesRequest.masterNodeTimeout()));

        client.admin().indices().aliases(indicesAliasesRequest, new ActionListener<IndicesAliasesResponse>() {

            @Override
            public void onResponse(IndicesAliasesResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    builder.field(Fields.OK, true);
                    builder.field(Fields.ACK, response.isAcknowledged());
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
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

    static class Fields {

        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString ACK = new XContentBuilderString("acknowledged");

    }
}
