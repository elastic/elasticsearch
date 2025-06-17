/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams.logs;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestSetLogStreamsEnabledAction extends BaseRestHandler {

    public static final Set<String> SUPPORTED_PARAMS = Set.of(RestUtils.REST_MASTER_TIMEOUT_PARAM, RestUtils.REST_TIMEOUT_PARAM);

    @Override
    public String getName() {
        return "streams_logs_set_enabled_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_streams/logs/_enable"), new Route(POST, "/_streams/logs/_disable"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final boolean enabled = request.path().endsWith("_enable");
        assert enabled || request.path().endsWith("_disable");

        LogsStreamsActivationToggleAction.Request activationRequest = new LogsStreamsActivationToggleAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request),
            enabled
        );

        return restChannel -> client.execute(
            LogsStreamsActivationToggleAction.INSTANCE,
            activationRequest,
            new RestToXContentListener<>(restChannel)
        );
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_PARAMS;
    }

}
