/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteLifecycleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(DELETE, "/_ilm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "ilm_delete_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String lifecycleName = restRequest.param("name");
        DeleteLifecycleAction.Request deleteLifecycleRequest = new DeleteLifecycleAction.Request(lifecycleName);
        deleteLifecycleRequest.timeout(restRequest.paramAsTime("timeout", deleteLifecycleRequest.timeout()));
        deleteLifecycleRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", deleteLifecycleRequest.masterNodeTimeout()));

        return channel -> client.execute(DeleteLifecycleAction.INSTANCE, deleteLifecycleRequest, new RestToXContentListener<>(channel));
    }
}
