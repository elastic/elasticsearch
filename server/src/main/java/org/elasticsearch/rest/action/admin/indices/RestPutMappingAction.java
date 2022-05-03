/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.internal.Requests.putMappingRequest;
import static org.elasticsearch.index.mapper.MapperService.isMappingSourceTyped;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutMappingAction extends BaseRestHandler {
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in put mapping request is deprecated. "
        + "Use typeless api instead";
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutMappingAction.class);

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_mapping/"),
            new Route(PUT, "/{index}/_mapping/"),
            new Route(POST, "/{index}/_mappings/"),
            new Route(PUT, "/{index}/_mappings/"),
            Route.builder(POST, "/{index}/{type}/_mapping").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/{index}/{type}/_mapping").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/_mapping/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/{index}/_mapping/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/_mapping/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_mapping/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/{type}/_mappings").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/{index}/{type}/_mappings").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/_mappings/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/{index}/_mappings/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/_mappings/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_mappings/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "put_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PutMappingRequest putMappingRequest = putMappingRequest(Strings.splitStringByCommaToArray(request.param("index")));

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
            if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
                deprecationLogger.compatibleCritical("put_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
            }
            final String type = request.param("type");
            if (includeTypeName == false && (type != null || isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap))) {
                throw new IllegalArgumentException(
                    "Types cannot be provided in put mapping requests, unless " + "the include_type_name parameter is set to true."
                );
            }

            Map<String, Object> mappingSource = prepareV7Mappings(includeTypeName, sourceAsMap);
            putMappingRequest.source(mappingSource);
        } else {
            if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap)) {
                throw new IllegalArgumentException("Types cannot be provided in put mapping requests");
            }
            putMappingRequest.source(sourceAsMap);
        }

        putMappingRequest.timeout(request.paramAsTime("timeout", putMappingRequest.timeout()));
        putMappingRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putMappingRequest.masterNodeTimeout()));
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        putMappingRequest.writeIndexOnly(request.paramAsBoolean("write_index_only", false));
        return channel -> client.admin().indices().putMapping(putMappingRequest, new RestToXContentListener<>(channel));
    }

    private static Map<String, Object> prepareV7Mappings(boolean includeTypeName, Map<String, Object> mappings) {
        if (includeTypeName && mappings != null && mappings.size() == 1) {
            String typeName = mappings.keySet().iterator().next();
            if (Strings.hasText(typeName) == false) {
                throw new IllegalArgumentException("name cannot be empty string");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> typelessMappings = (Map<String, Object>) mappings.get(typeName);
            return typelessMappings;
        }
        return mappings;
    }

}
