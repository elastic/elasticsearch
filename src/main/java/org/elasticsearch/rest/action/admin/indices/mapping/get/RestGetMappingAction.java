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
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetMappingAction extends BaseRestHandler {

    @Inject
    public RestGetMappingAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_mapping", this);
        controller.registerHandler(GET, "/{index}/_mapping", this);
        controller.registerHandler(GET, "/{index}/{type}/_mapping", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        boolean local = request.paramAsBooleanOptional("local", false);
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices).types(types).local(local);
        client.admin().indices().getMappings(getMappingsRequest, new ActionListener<GetMappingsResponse>() {

            @Override
            public void onResponse(GetMappingsResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();

                    ImmutableMap<String, ImmutableMap<String, MappingMetaData>> mappingsByIndex = response.getMappings();
                    if (mappingsByIndex.isEmpty()) {
                        if (indices.length != 0 && types.length != 0) {
                            channel.sendResponse(new XContentThrowableRestResponse(request, new TypeMissingException(new Index(indices[0]), types[0])));
                        } else if (indices.length != 0) {
                            channel.sendResponse(new XContentThrowableRestResponse(request, new IndexMissingException(new Index(indices[0]))));
                        } else if (types.length != 0) {
                            channel.sendResponse(new XContentThrowableRestResponse(request, new TypeMissingException(new Index("_all"), types[0])));
                        } else {
                            builder.endObject();
                            channel.sendResponse(new XContentRestResponse(request, OK, builder));
                        }
                        return;
                    }

                    for (Map.Entry<String, ImmutableMap<String, MappingMetaData>> indexEntry : mappingsByIndex.entrySet()) {
                        builder.startObject(indexEntry.getKey(), XContentBuilder.FieldCaseConversion.NONE);
                        for (Map.Entry<String, MappingMetaData> typeEntry : indexEntry.getValue().entrySet()) {
                            builder.field(typeEntry.getKey());
                            builder.map(typeEntry.getValue().sourceAsMap());
                        }
                        builder.endObject();
                    }

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
}
