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
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.index.mapper.MapperService.isMappingSourceTyped;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutMappingAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutMappingAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in put "
        + "mapping requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(POST, "/{index}/_mapping/"),
                new Route(PUT, "/{index}/_mapping/"),
                new Route(POST, "/{index}/{type}/_mapping"),
                new Route(PUT, "/{index}/{type}/_mapping"),
                new Route(POST, "/{index}/_mapping/{type}"),
                new Route(PUT, "/{index}/_mapping/{type}"),
                new Route(POST, "/_mapping/{type}"),
                new Route(PUT, "/_mapping/{type}"),
                new Route(POST, "/{index}/_mappings/"),
                new Route(PUT, "/{index}/_mappings/"),
                new Route(POST, "/{index}/{type}/_mappings"),
                new Route(PUT, "/{index}/{type}/_mappings"),
                new Route(POST, "/{index}/_mappings/{type}"),
                new Route(PUT, "/{index}/_mappings/{type}"),
                new Route(POST, "/_mappings/{type}"),
                new Route(PUT, "/_mappings/{type}")
            )
        );
    }

    @Override
    public String getName() {
        return "put_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.critical(DeprecationCategory.TYPES, "put_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        PutMappingRequest putMappingRequest = putMappingRequest(Strings.splitStringByCommaToArray(request.param("index")));

        final String type = request.param("type");
        putMappingRequest.type(includeTypeName ? type : MapperService.SINGLE_MAPPING_NAME);

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();
        if (includeTypeName == false && (type != null || isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap))) {
            throw new IllegalArgumentException(
                "Types cannot be provided in put mapping requests, unless " + "the include_type_name parameter is set to true."
            );
        }

        putMappingRequest.source(sourceAsMap);
        putMappingRequest.timeout(request.paramAsTime("timeout", putMappingRequest.timeout()));
        putMappingRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putMappingRequest.masterNodeTimeout()));
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        putMappingRequest.writeIndexOnly(request.paramAsBoolean("write_index_only", false));
        return channel -> client.admin().indices().putMapping(putMappingRequest, new RestToXContentListener<>(channel));
    }
}
