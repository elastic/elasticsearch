/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSyncedFlushAction extends BaseRestHandler {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RestSyncedFlushAction.class);

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_flush/synced"),
            new Route(POST, "/_flush/synced"),
            new Route(GET, "/{index}/_flush/synced"),
            new Route(POST, "/{index}/_flush/synced"));
    }

    @Override
    public String getName() {
        return "synced_flush_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DEPRECATION_LOGGER.deprecate(DeprecationCategory.API, "synced_flush",
            "Synced flush was removed and a normal flush was performed instead. This transition will be removed in a future version.");
        final FlushRequest flushRequest = new FlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        flushRequest.indicesOptions(IndicesOptions.fromRequest(request, flushRequest.indicesOptions()));
        return channel -> client.admin().indices().flush(flushRequest, new SimulateSyncedFlushResponseListener(channel));
    }

    static final class SimulateSyncedFlushResponseListener extends RestBuilderListener<FlushResponse> {

        SimulateSyncedFlushResponseListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(FlushResponse flushResponse, XContentBuilder builder) throws Exception {
            builder.startObject();
            buildSyncedFlushResponse(builder, flushResponse);
            builder.endObject();
            final RestStatus restStatus = flushResponse.getFailedShards() == 0 ? RestStatus.OK : RestStatus.CONFLICT;
            return new BytesRestResponse(restStatus, builder);
        }

        private void buildSyncedFlushResponse(XContentBuilder builder, FlushResponse flushResponse) throws IOException {
            builder.startObject("_shards");
            builder.field("total", flushResponse.getTotalShards());
            builder.field("successful", flushResponse.getSuccessfulShards());
            builder.field("failed", flushResponse.getFailedShards());
            // can't serialize the detail of each index as we don't have the shard count per index.
            builder.endObject();
        }
    }
}
