/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.frozen.rest.action;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

public final class RestFreezeIndexAction extends BaseRestHandler {

    public RestFreezeIndexAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_freeze", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_unfreeze", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        boolean freeze = request.path().endsWith("/_freeze");
        FreezeRequest freezeRequest = new FreezeRequest(Strings.splitStringByCommaToArray(request.param("index")));
        freezeRequest.timeout(request.paramAsTime("timeout", freezeRequest.timeout()));
        freezeRequest.masterNodeTimeout(request.paramAsTime("master_timeout", freezeRequest.masterNodeTimeout()));
        freezeRequest.indicesOptions(IndicesOptions.fromRequest(request, freezeRequest.indicesOptions()));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            freezeRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        freezeRequest.setFreeze(freeze);
        return channel -> client.execute(FreezeIndexAction.INSTANCE, freezeRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "freeze_index";
    }
}
