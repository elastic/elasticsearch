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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
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

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetFieldMappingAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestGetFieldMappingAction.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());
    public static final String INCLUDE_TYPE_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get "
        + "field mapping requests is deprecated. The parameter will be removed in the next major version.";
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in get field mapping request is deprecated. "
        + "Use typeless api instead";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_mapping/field/{fields}"),
            new Route(GET, "/{index}/_mapping/field/{fields}"),
            Route.builder(GET, "/_mapping/{type}/field/{fields}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(GET, "/{index}/{type}/_mapping/field/{fields}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(GET, "/{index}/_mapping/{type}/field/{fields}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "get_field_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] fields = Strings.splitStringByCommaToArray(request.param("fields"));

        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
                deprecationLogger.compatibleCritical("get_field_mapping_with_types", INCLUDE_TYPE_DEPRECATION_MESSAGE);
            }
            boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
            final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
            if (includeTypeName == false && types.length > 0) {
                throw new IllegalArgumentException("Types cannot be specified unless include_type_name" + " is set to true.");
            }

            if (request.hasParam("local")) {
                request.param("local");
                deprecationLogger.compatibleCritical(
                    "get_field_mapping_local",
                    "Use [local] in get field mapping requests is deprecated. " + "The parameter will be removed in the next major version"
                );
            }
        }

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(indices).fields(fields).includeDefaults(request.paramAsBoolean("include_defaults", false));
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        return channel -> client.admin().indices().getFieldMappings(getMappingsRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(GetFieldMappingsResponse response, XContentBuilder builder) throws Exception {
                Map<String, Map<String, FieldMappingMetadata>> mappingsByIndex = response.mappings();

                RestStatus status = OK;
                if (mappingsByIndex.isEmpty() && fields.length > 0) {
                    status = NOT_FOUND;
                }
                response.toXContent(builder, request);
                return new BytesRestResponse(status, builder);
            }
        });
    }

}
