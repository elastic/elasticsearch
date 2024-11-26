/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.frozen.rest.action;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public final class RestFreezeIndexAction extends BaseRestHandler {

    private static final String FREEZE_REMOVED = "It is no longer possible to freeze indices, but existing frozen indices can still be "
        + "unfrozen";

    private static final String UNFREEZE_DEPRECATED = "Frozen indices are deprecated because they provide no benefit given improvements "
        + "in heap memory utilization. They will be removed in a future release.";

    @UpdateForV9(owner = UpdateForV9.Owner.DISTRIBUTED_INDEXING)
    // these routes were ".deprecated" in RestApiVersion.V_8 which will require use of REST API compatibility headers to access
    // this API in v9. It is unclear if this was intentional for v9, and the code has been updated to ".deprecateAndKeep" which will
    // continue to emit deprecations warnings but will not require any special headers to access the API in v9.
    // Please review and update the code and tests as needed. The original code remains commented out below for reference.
    @Override
    public List<Route> routes() {
        return List.of(
            // Route.builder(POST, "/{index}/_unfreeze").deprecated(UNFREEZE_DEPRECATED, RestApiVersion.V_8).build()
            Route.builder(POST, "/{index}/_unfreeze").deprecateAndKeep(UNFREEZE_DEPRECATED).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final var freezeRequest = new FreezeRequest(
            getMasterNodeTimeout(request),
            getAckTimeout(request),
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        freezeRequest.indicesOptions(IndicesOptions.fromRequest(request, freezeRequest.indicesOptions()));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            freezeRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        freezeRequest.setFreeze(false);
        return channel -> client.execute(FreezeIndexAction.INSTANCE, freezeRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "freeze_index";
    }
}
