/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.options.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.datastreams.options.action.GetDataStreamOptionsAction;
import org.elasticsearch.datastreams.rest.RestGetDataStreamsAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDataStreamOptionsAction extends BaseRestHandler {

    private static final Set<String> CAPABILITIES = Set.of(RestGetDataStreamsAction.FAILURES_LIFECYCLE_API_CAPABILITY);

    @Override
    public String getName() {
        return "get_data_stream_options_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_data_stream/{name}/_options"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetDataStreamOptionsAction.Request getDataStreamOptionsRequest = new GetDataStreamOptionsAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        getDataStreamOptionsRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        getDataStreamOptionsRequest.indicesOptions(IndicesOptions.fromRequest(request, getDataStreamOptionsRequest.indicesOptions()));
        return channel -> client.execute(
            GetDataStreamOptionsAction.INSTANCE,
            getDataStreamOptionsRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }
}
