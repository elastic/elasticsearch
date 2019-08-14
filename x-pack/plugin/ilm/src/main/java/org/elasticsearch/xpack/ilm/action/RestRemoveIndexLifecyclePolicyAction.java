/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;

public class RestRemoveIndexLifecyclePolicyAction extends BaseRestHandler {

    public RestRemoveIndexLifecyclePolicyAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_ilm/remove", this);
    }

    @Override
    public String getName() {
        return "ilm_remove_policy_for_index_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String[] indexes = Strings.splitStringByCommaToArray(restRequest.param("index"));
        RemoveIndexLifecyclePolicyAction.Request changePolicyRequest = new RemoveIndexLifecyclePolicyAction.Request(indexes);
        changePolicyRequest.masterNodeTimeout(restRequest.paramAsTime("master_timeout", changePolicyRequest.masterNodeTimeout()));
        changePolicyRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, changePolicyRequest.indicesOptions()));

        return channel ->
                client.execute(RemoveIndexLifecyclePolicyAction.INSTANCE, changePolicyRequest, new RestToXContentListener<>(channel));
    }
}
