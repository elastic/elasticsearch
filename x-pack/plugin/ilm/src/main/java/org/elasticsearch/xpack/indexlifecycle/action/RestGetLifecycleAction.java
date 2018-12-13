/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;

import java.io.IOException;

public class RestGetLifecycleAction extends BaseRestHandler {

    public RestGetLifecycleAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "/_ilm/policy", this);
        controller.registerHandler(RestRequest.Method.GET, "/_ilm/policy/{name}", this);
    }

    @Override
    public String getName() {
        return "ilm_get_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String[] lifecycleNames = Strings.splitStringByCommaToArray(restRequest.param("name"));
        GetLifecycleAction.Request getLifecycleRequest = new GetLifecycleAction.Request(lifecycleNames);
        getLifecycleRequest.timeout(restRequest.paramAsTime("timeout", getLifecycleRequest.timeout()));
        getLifecycleRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", getLifecycleRequest.masterNodeTimeout()));

        return channel -> client.execute(GetLifecycleAction.INSTANCE, getLifecycleRequest, new RestToXContentListener<>(channel));
    }
}
