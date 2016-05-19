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

package org.elasticsearch.rest.action.admin.indices.mapping.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetMappingAction extends BaseRestHandler {

    @Inject
    public RestGetMappingAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/_mapping", this);
        controller.registerHandler(GET, "/{index}/_mappings/{type}", this);
        controller.registerHandler(GET, "/{index}/_mapping/{type}", this);
        controller.registerHandler(GET, "/_mapping/{type}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices).types(types);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        client.admin().indices().getMappings(getMappingsRequest, new RestBuilderListener<GetMappingsResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetMappingsResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappingsByIndex = response.getMappings();
                if (mappingsByIndex.isEmpty()) {
                    if (indices.length != 0 && types.length != 0) {
                        return new BytesRestResponse(OK, builder.endObject());
                    } else if (indices.length != 0) {
                        return new BytesRestResponse(channel, new IndexNotFoundException(indices[0]));
                    } else if (types.length != 0) {
                        return new BytesRestResponse(channel, new TypeMissingException("_all", types[0]));
                    } else {
                        return new BytesRestResponse(OK, builder.endObject());
                    }
                }

                for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappingsByIndex) {
                    if (indexEntry.value.isEmpty()) {
                        continue;
                    }
                    builder.startObject(indexEntry.key);
                    builder.startObject(Fields.MAPPINGS);
                    for (ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                        builder.field(typeEntry.key);
                        builder.map(typeEntry.value.sourceAsMap());
                    }
                    builder.endObject();
                    builder.endObject();
                }

                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    static class Fields {
        static final String MAPPINGS = "mappings";
    }
}
