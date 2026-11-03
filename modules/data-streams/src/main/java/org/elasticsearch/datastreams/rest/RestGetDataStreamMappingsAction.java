/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.GetDataStreamMappingsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDataStreamMappingsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(RestUtils.REST_MASTER_TIMEOUT_PARAM);
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Set.copyOf(Sets.union(SUPPORTED_QUERY_PARAMETERS, Set.of("name")));

    @Override
    public String getName() {
        return "get_data_stream_mappings_action";
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    public Set<String> allSupportedParameters() {
        return ALL_SUPPORTED_PARAMETERS;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_data_stream/{name}/_mappings"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetDataStreamMappingsAction.Request getDataStreamRequest = new GetDataStreamMappingsAction.Request(
            RestUtils.getMasterNodeTimeout(request)
        ).indices(Strings.splitStringByCommaToArray(request.param("name")));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetDataStreamMappingsAction.INSTANCE,
            getDataStreamRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }
}
