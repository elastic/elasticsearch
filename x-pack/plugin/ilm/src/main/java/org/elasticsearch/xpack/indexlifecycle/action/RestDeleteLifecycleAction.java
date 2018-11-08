/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.indexlifecycle.action.DeleteLifecycleAction;

import java.io.IOException;

public class RestDeleteLifecycleAction extends BaseRestHandler {

    public RestDeleteLifecycleAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.DELETE, "/_ilm/policy/{name}", this);
    }

    @Override
    public String getName() {
        return "ilm_delete_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String lifecycleName = restRequest.param("name");
        DeleteLifecycleAction.Request deleteLifecycleRequest = new DeleteLifecycleAction.Request(lifecycleName);
        deleteLifecycleRequest.timeout(restRequest.paramAsTime("timeout", deleteLifecycleRequest.timeout()));
        deleteLifecycleRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", deleteLifecycleRequest.masterNodeTimeout()));

        return channel -> client.execute(DeleteLifecycleAction.INSTANCE, deleteLifecycleRequest, new RestToXContentListener<>(channel));
    }
}
