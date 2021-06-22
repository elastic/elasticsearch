/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.frozen.rest.action;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public final class RestFreezeIndexAction extends BaseRestHandler {

    public static final String DEPRECATION_WARNING = "Frozen indices are deprecated because they provide no benefit given improvements "
        + "in heap memory utilization. They will be removed in a future release.";
    private static final RestApiVersion DEPRECATION_VERSION = RestApiVersion.V_8;

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/{index}/_freeze").deprecated(DEPRECATION_WARNING, DEPRECATION_VERSION).build(),
            Route.builder(POST, "/{index}/_unfreeze").deprecated(DEPRECATION_WARNING, DEPRECATION_VERSION).build()
        );
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
