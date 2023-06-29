/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetLifecycleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_ilm/policy"), new Route(GET, "/_ilm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "ilm_get_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String[] lifecycleNames = Strings.splitStringByCommaToArray(restRequest.param("name"));
        GetLifecycleAction.Request getLifecycleRequest = new GetLifecycleAction.Request(lifecycleNames);
        getLifecycleRequest.timeout(restRequest.paramAsTime("timeout", getLifecycleRequest.timeout()));
        getLifecycleRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", getLifecycleRequest.masterNodeTimeout()));

        return channel -> client.execute(GetLifecycleAction.INSTANCE, getLifecycleRequest, new RestChunkedToXContentListener<>(channel));
    }
}
