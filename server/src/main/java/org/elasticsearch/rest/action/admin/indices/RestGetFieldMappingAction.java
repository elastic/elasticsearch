/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetFieldMappingAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestGetFieldMappingAction.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get "
        + "field mapping requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_mapping/field/{fields}"),
                new Route(GET, "/_mapping/{type}/field/{fields}"),
                new Route(GET, "/{index}/_mapping/field/{fields}"),
                new Route(GET, "/{index}/{type}/_mapping/field/{fields}"),
                new Route(GET, "/{index}/_mapping/{type}/field/{fields}")
            )
        );
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
            throw new IllegalArgumentException("Types cannot be specified unless include_type_name" + " is set to true.");
        }
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.critical(DeprecationCategory.TYPES, "get_field_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(indices).types(types).fields(fields).includeDefaults(request.paramAsBoolean("include_defaults", false));
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));

        if (request.hasParam("local")) {
            deprecationLogger.critical(
                DeprecationCategory.API,
                "get_field_mapping_local",
                "Use [local] in get field mapping requests is deprecated. " + "The parameter will be removed in the next major version"
            );
        }
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        return channel -> client.admin()
            .indices()
            .getFieldMappings(getMappingsRequest, new RestBuilderListener<GetFieldMappingsResponse>(channel) {
                @Override
                public RestResponse buildResponse(GetFieldMappingsResponse response, XContentBuilder builder) throws Exception {
                    Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappingsByIndex = response.mappings();

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
    private boolean isFieldMappingMissingField(Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappingsByIndex) {
        if (mappingsByIndex.size() != 1) {
            return false;
        }

        for (Map<String, Map<String, FieldMappingMetadata>> value : mappingsByIndex.values()) {
            for (Map<String, FieldMappingMetadata> fieldValue : value.values()) {
                for (Map.Entry<String, FieldMappingMetadata> fieldMappingMetadataEntry : fieldValue.entrySet()) {
                    if (fieldMappingMetadataEntry.getValue().isNull()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
