/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

@ServerlessScope(Scope.PUBLIC)
public class RestGetMappingAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetMappingAction.class);
    public static final String INCLUDE_TYPE_DEPRECATION_MSG = "[types removal] Using include_type_name in get"
        + " mapping requests is deprecated. The parameter will be removed in the next major version.";
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in get mapping request is deprecated. "
        + "Use typeless api instead";

    public RestGetMappingAction() {}

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_mapping"),
            new Route(GET, "/_mappings"),
            Route.builder(GET, "/{index}/{type}/_mapping").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            new Route(GET, "/{index}/_mapping"),
            new Route(GET, "/{index}/_mappings"),
            Route.builder(GET, "/{index}/_mappings/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(GET, "/{index}/_mapping/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(HEAD, "/{index}/_mapping/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(GET, "/_mapping/{type}").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
                request.param(INCLUDE_TYPE_NAME_PARAMETER);
                deprecationLogger.compatibleCritical("get_mapping_with_types", INCLUDE_TYPE_DEPRECATION_MSG);
            }
            final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
            if (request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY) == false && types.length > 0) {
                throw new IllegalArgumentException(
                    "Types cannot be provided in get mapping requests, unless include_type_name is set to true."
                );
            }
            if (request.method().equals(HEAD)) {
                deprecationLogger.compatibleCritical(
                    "get_mapping_types_removal",
                    "Type exists requests are deprecated, as types have been deprecated."
                );
            }
        }
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        final TimeValue timeout = request.paramAsTime("master_timeout", getMappingsRequest.masterNodeTimeout());
        getMappingsRequest.masterNodeTimeout(timeout);
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        final HttpChannel httpChannel = request.getHttpChannel();
        return channel -> new RestCancellableNodeClient(client, httpChannel).admin()
            .indices()
            .getMappings(getMappingsRequest, new RestRefCountedChunkedToXContentListener<>(channel));
    }
}
