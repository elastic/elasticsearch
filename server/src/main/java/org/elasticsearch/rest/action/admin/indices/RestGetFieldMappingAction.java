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

package org.elasticsearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetFieldMappingAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestGetFieldMappingAction.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get " +
        "field mapping requests is deprecated. The parameter will be removed in the next major version.";

    public RestGetFieldMappingAction(RestController controller) {
        controller.registerHandler(GET, "/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/_mapping/{type}/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/{type}/_mapping/field/{fields}", this);
        controller.registerHandler(GET, "/{index}/_mapping/{type}/field/{fields}", this);
    }

    @Override
    public String getName() {
        return "get_field_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
        final String[] fields = Strings.splitStringByCommaToArray(request.param("fields"));

        boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
        if (includeTypeName == false && types.length > 0) {
            throw new IllegalArgumentException("Types cannot be specified unless include_type_name" +
                " is set to true.");
        }
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.deprecatedAndMaybeLog("get_field_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(indices).types(types).fields(fields).includeDefaults(request.paramAsBoolean("include_defaults", false));
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        return channel ->
                client.admin().indices().getFieldMappings(getMappingsRequest, new RestBuilderListener<GetFieldMappingsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(GetFieldMappingsResponse response, XContentBuilder builder) throws Exception {
                        Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappingsByIndex = response.mappings();

                        boolean isPossibleSingleFieldRequest = indices.length == 1 && types.length == 1 && fields.length == 1;
                        if (isPossibleSingleFieldRequest && isFieldMappingMissingField(mappingsByIndex)) {
                            return new BytesRestResponse(OK, builder.startObject().endObject());
                        }

                        RestStatus status = OK;
                        if (mappingsByIndex.isEmpty() && fields.length > 0) {
                            status = NOT_FOUND;
                        }
                        response.toXContent(builder, request);
                        return new BytesRestResponse(status, builder);
                    }
                });
    }

    /**
     * Helper method to find out if the only included fieldmapping metadata is typed NULL, which means
     * that type and index exist, but the field did not
     */
    private boolean isFieldMappingMissingField(Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappingsByIndex) {
        if (mappingsByIndex.size() != 1) {
            return false;
        }

        for (Map<String, Map<String, FieldMappingMetaData>> value : mappingsByIndex.values()) {
            for (Map<String, FieldMappingMetaData> fieldValue : value.values()) {
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
