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

package org.elasticsearch.rest.action.admin.indices.mapping.get;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetFieldMappingAction extends BaseRestHandler {

    @Inject
    public RestGetFieldMappingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/{type}/_mapping/field/{fields}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        boolean local = request.paramAsBooleanOptional("local", false);
        final String[] fields = Strings.splitStringByCommaToArray(request.param("fields"));

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(indices).types(types).local(local).fields(fields).includeDefaults(request.paramAsBoolean("include_defaults", false));
        client.admin().indices().getFieldMappings(getMappingsRequest, new ActionListener<GetFieldMappingsResponse>() {

            @SuppressWarnings("unchecked")
            @Override
            public void onResponse(GetFieldMappingsResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();

                    ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> mappingsByIndex = response.mappings();
                    RestStatus status = OK;
                    if (mappingsByIndex.isEmpty() && fields.length > 0) {
                        status = NOT_FOUND;
                    }
                    response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
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

}
