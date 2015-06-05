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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetFieldMappingAction extends BaseRestHandler {

    @Inject
    public RestGetFieldMappingAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/_mapping/{type}/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/{type}/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/_mapping/{type}/field/{fields}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
        final String[] fields = Strings.splitStringByCommaToArray(request.param("fields"));
        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(indices).types(types).fields(fields).includeDefaults(request.paramAsBoolean("include_defaults", false));
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        client.admin().indices().getFieldMappings(getMappingsRequest, new RestBuilderListener<GetFieldMappingsResponse>(channel) {

            @SuppressWarnings("unchecked")
            @Override
            public RestResponse buildResponse(GetFieldMappingsResponse response, XContentBuilder builder) throws Exception {
                ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> mappingsByIndex = response.mappings();

                boolean isPossibleSingleFieldRequest = indices.length == 1 && types.length == 1 && fields.length == 1;
                if (isPossibleSingleFieldRequest && isFieldMappingMissingField(mappingsByIndex)) {
                    return new BytesRestResponse(OK, builder.startObject().endObject());
                }

                RestStatus status = OK;
                if (mappingsByIndex.isEmpty() && fields.length > 0) {
                    status = NOT_FOUND;
                }
                builder.startObject();
                response.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }
        });
    }

    /**
     * Helper method to find out if the only included fieldmapping metadata is typed NULL, which means
     * that type and index exist, but the field did not
     */
    private boolean isFieldMappingMissingField(ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> mappingsByIndex) throws IOException {
        if (mappingsByIndex.size() != 1) {
            return false;
        }

        for (ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>> value : mappingsByIndex.values()) {
            for (ImmutableMap<String, FieldMappingMetaData> fieldValue : value.values()) {
                for (Map.Entry<String, FieldMappingMetaData> fieldMappingMetaDataEntry : fieldValue.entrySet()) {
                    if (fieldMappingMetaDataEntry.getValue().isNull()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
