/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.dlm.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.dlm.action.GetDataLifecycleAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetDataLifecycleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_data_lifecycles_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_data_stream/_lifecycle"), new Route(GET, "/_data_stream/{name}/_lifecycle"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetDataLifecycleAction.Request getDataStreamsRequest = new GetDataLifecycleAction.Request(
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        getDataStreamsRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        getDataStreamsRequest.indicesOptions(IndicesOptions.fromRequest(request, getDataStreamsRequest.indicesOptions()));
        return channel -> client.execute(GetDataLifecycleAction.INSTANCE, getDataStreamsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }
}
