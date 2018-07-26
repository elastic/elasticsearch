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
import org.elasticsearch.xpack.core.indexlifecycle.action.SetIndexPolicyAction;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexPolicyRequest;

import java.io.IOException;

public class RestSetIndexPolicyAction extends BaseRestHandler {

    public RestSetIndexPolicyAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, "_lifecycle/{new_policy}", this);
        controller.registerHandler(RestRequest.Method.PUT, "{index}/_lifecycle/{new_policy}", this);
    }

    @Override
    public String getName() {
        return "xpack_set_index_policy_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String[] indexes = Strings.splitStringByCommaToArray(restRequest.param("index"));
        String newPolicyName = restRequest.param("new_policy");
        SetIndexPolicyRequest changePolicyRequest = new SetIndexPolicyRequest(newPolicyName, indexes);
        changePolicyRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", changePolicyRequest.masterNodeTimeout()));

        return channel -> client.execute(SetIndexPolicyAction.INSTANCE, changePolicyRequest, new RestToXContentListener<>(channel));
    }
}
