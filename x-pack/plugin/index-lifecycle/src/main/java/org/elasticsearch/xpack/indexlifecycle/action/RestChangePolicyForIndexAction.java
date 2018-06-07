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
import org.elasticsearch.xpack.core.indexlifecycle.action.ChangePolicyForIndexAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycle;

import java.io.IOException;

public class RestChangePolicyForIndexAction extends BaseRestHandler {

    public RestChangePolicyForIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "_" + IndexLifecycle.NAME + "/change_policy/{new_policy}", this);
        controller.registerHandler(RestRequest.Method.GET, "{index}/_" + IndexLifecycle.NAME + "/change_policy/{new_policy}", this);
    }

    @Override
    public String getName() {
        return "xpack_lifecycle_put_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String[] indexes = Strings.splitStringByCommaToArray(restRequest.param("index"));
        String newPolicyName = restRequest.param("new_policy");
        ChangePolicyForIndexAction.Request changePolicyRequest = new ChangePolicyForIndexAction.Request(newPolicyName, indexes);
        changePolicyRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", changePolicyRequest.masterNodeTimeout()));

        return channel -> client.execute(ChangePolicyForIndexAction.INSTANCE, changePolicyRequest, new RestToXContentListener<>(channel));
    }
}