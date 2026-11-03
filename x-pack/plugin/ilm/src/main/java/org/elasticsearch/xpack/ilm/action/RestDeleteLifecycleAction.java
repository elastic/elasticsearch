/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.REST_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestDeleteLifecycleAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(REST_MASTER_TIMEOUT_PARAM, REST_TIMEOUT_PARAM);
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Set.copyOf(Sets.union(SUPPORTED_QUERY_PARAMETERS, Set.of("name")));

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_ilm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "ilm_delete_action";
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
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String lifecycleName = restRequest.param("name");
        DeleteLifecycleAction.Request deleteLifecycleRequest = new DeleteLifecycleAction.Request(
            getMasterNodeTimeout(restRequest),
            getAckTimeout(restRequest),
            lifecycleName
        );

        return channel -> client.execute(DeleteLifecycleAction.INSTANCE, deleteLifecycleRequest, new RestToXContentListener<>(channel));
    }
}
