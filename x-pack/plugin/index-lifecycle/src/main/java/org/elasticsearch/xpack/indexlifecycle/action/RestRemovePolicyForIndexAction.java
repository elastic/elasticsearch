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
import org.elasticsearch.xpack.core.indexlifecycle.action.RemovePolicyForIndexAction;

import java.io.IOException;

public class RestRemovePolicyForIndexAction extends BaseRestHandler {

    public RestRemovePolicyForIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.DELETE, "_lifecycle", this);
        controller.registerHandler(RestRequest.Method.DELETE, "{index}/_lifecycle", this);
    }

    @Override
    public String getName() {
        return "xpack_remove_policy_for_index_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String[] indexes = Strings.splitStringByCommaToArray(restRequest.param("index"));
        RemovePolicyForIndexAction.Request changePolicyRequest = new RemovePolicyForIndexAction.Request(indexes);
        changePolicyRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", changePolicyRequest.masterNodeTimeout()));

        return channel -> client.execute(RemovePolicyForIndexAction.INSTANCE, changePolicyRequest, new RestToXContentListener<>(channel));
    }
}