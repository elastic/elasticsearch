/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestGetSnapshotLifecycleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_slm/policy"), new Route(GET, "/_slm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "slm_get_lifecycle";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final var req = new GetSnapshotLifecycleAction.Request(
            getMasterNodeTimeout(request),
            getAckTimeout(request),
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        return channel -> client.execute(GetSnapshotLifecycleAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
