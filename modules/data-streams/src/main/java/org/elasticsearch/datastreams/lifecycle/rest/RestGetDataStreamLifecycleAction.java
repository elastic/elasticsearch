/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.action.datastreams.lifecycle.GetDataStreamLifecycleAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
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
public class RestGetDataStreamLifecycleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_data_lifecycles_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_data_stream/{name}/_lifecycle"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        getDataLifecycleRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        getDataLifecycleRequest.indicesOptions(IndicesOptions.fromRequest(request, getDataLifecycleRequest.indicesOptions()));
        return channel -> client.execute(
            GetDataStreamLifecycleAction.INSTANCE,
            getDataLifecycleRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(DataStreamLifecycle.EFFECTIVE_RETENTION_REST_API_CAPABILITY);
    }
}
