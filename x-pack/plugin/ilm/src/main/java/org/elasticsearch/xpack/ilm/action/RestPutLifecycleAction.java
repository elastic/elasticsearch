/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestPutLifecycleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_ilm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "ilm_put_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final PutLifecycleRequest putLifecycleRequest;
        try (var parser = restRequest.contentParser()) {
            putLifecycleRequest = PutLifecycleRequest.parseRequest(new PutLifecycleRequest.Factory() {
                @Override
                public PutLifecycleRequest create(LifecyclePolicy lifecyclePolicy) {
                    return new PutLifecycleRequest(getMasterNodeTimeout(restRequest), getAckTimeout(restRequest), lifecyclePolicy);
                }

                @Override
                public String getPolicyName() {
                    return restRequest.param("name");
                }
            }, parser);
        }
        return channel -> client.execute(ILMActions.PUT, putLifecycleRequest, new RestToXContentListener<>(channel));
    }
}
