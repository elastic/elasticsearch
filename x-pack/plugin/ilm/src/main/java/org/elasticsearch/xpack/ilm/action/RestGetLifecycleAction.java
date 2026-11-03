/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_PARAM;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestGetLifecycleAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(REST_MASTER_TIMEOUT_PARAM);
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Set.copyOf(Sets.union(SUPPORTED_QUERY_PARAMETERS, Set.of("name")));

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_ilm/policy"), new Route(GET, "/_ilm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "ilm_get_action";
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
        String[] lifecycleNames = Strings.splitStringByCommaToArray(restRequest.param("name"));
        GetLifecycleAction.Request getLifecycleRequest = new GetLifecycleAction.Request(getMasterNodeTimeout(restRequest), lifecycleNames);

        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            GetLifecycleAction.INSTANCE,
            getLifecycleRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }
}
