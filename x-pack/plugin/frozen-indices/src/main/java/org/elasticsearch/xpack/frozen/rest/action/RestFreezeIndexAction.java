/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.frozen.rest.action;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.GONE;

public final class RestFreezeIndexAction extends BaseRestHandler {

    private static final String FREEZE_REMOVED = "It is no longer possible to freeze indices, but existing frozen indices can still be "
        + "unfrozen";

    private static final String UNFREEZE_DEPRECATED = "Frozen indices are deprecated because they provide no benefit given improvements "
        + "in heap memory utilization. They will be removed in a future release.";

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/{index}/_freeze").deprecated(FREEZE_REMOVED, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/_unfreeze").deprecated(UNFREEZE_DEPRECATED, RestApiVersion.V_8).build()
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.path().endsWith("/_freeze")) {
            // translate to a get indices request, so that we'll 404 on non-existent indices
            final GetIndexRequest getIndexRequest = new GetIndexRequest();
            getIndexRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            getIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getIndexRequest.masterNodeTimeout()));
            getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getIndexRequest.indicesOptions()));
            return channel -> client.admin().indices().getIndex(getIndexRequest, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetIndexResponse getIndexResponse, XContentBuilder builder) throws Exception {
                    builder.close();
                    // but if the index *does* exist, we still just respond with 410 -- there's no such thing as _freeze anymore
                    return new BytesRestResponse(channel, GONE, new UnsupportedOperationException(FREEZE_REMOVED));
                }
            });
        }

        FreezeRequest freezeRequest = new FreezeRequest(Strings.splitStringByCommaToArray(request.param("index")));
        freezeRequest.timeout(request.paramAsTime("timeout", freezeRequest.timeout()));
        freezeRequest.masterNodeTimeout(request.paramAsTime("master_timeout", freezeRequest.masterNodeTimeout()));
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
