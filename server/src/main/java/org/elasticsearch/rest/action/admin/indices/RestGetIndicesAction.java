/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;


import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

/**
 * The REST handler for get index and head index APIs.
 */
public class RestGetIndicesAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetIndicesAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using `include_type_name` in get indices requests"
        + " is deprecated. The parameter will be removed in the next major version.";

    private static final Set<String> COMPATIBLE_RESPONSE_PARAMS = Collections
        .unmodifiableSet(Stream.concat(Collections.singleton(INCLUDE_TYPE_NAME_PARAMETER).stream(), Settings.FORMAT_PARAMS.stream())
            .collect(Collectors.toSet()));

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}"),
            new Route(HEAD, "/{index}"));
    }

    @Override
    public String getName() {
        return "get_indices_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // starting with 7.0 we don't include types by default in the response to GET requests
        if (request.getRestApiVersion() == RestApiVersion.V_7 &&
            request.hasParam(INCLUDE_TYPE_NAME_PARAMETER) && request.method().equals(GET)) {
            deprecationLogger.compatibleApiWarning("get_indices_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indices);
        getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getIndexRequest.indicesOptions()));
        getIndexRequest.local(request.paramAsBoolean("local", getIndexRequest.local()));
        getIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getIndexRequest.masterNodeTimeout()));
        getIndexRequest.humanReadable(request.paramAsBoolean("human", false));
        getIndexRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        return channel -> client.admin().indices().getIndex(getIndexRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parameters used for controlling the response and thus might not be consumed during
     * preparation of the request execution in {@link BaseRestHandler#prepareRequest(RestRequest, NodeClient)}.
     */
    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    protected Set<String> responseParams(RestApiVersion restApiVersion) {
        if(restApiVersion == RestApiVersion.V_7){
            return COMPATIBLE_RESPONSE_PARAMS;
        } else {
            return responseParams();
        }
    }
}
