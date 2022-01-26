/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSyncedFlushAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_flush/synced"),
                new Route(POST, "/_flush/synced"),
                new Route(GET, "/{index}/_flush/synced"),
                new Route(POST, "/{index}/_flush/synced")
            )
        );
    }

    @Override
    public String getName() {
        return "synced_flush_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.lenientExpandOpen());
        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        syncedFlushRequest.indicesOptions(indicesOptions);
        return channel -> client.admin().indices().syncedFlush(syncedFlushRequest, new RestBuilderListener<SyncedFlushResponse>(channel) {
            @Override
            public RestResponse buildResponse(SyncedFlushResponse results, XContentBuilder builder) throws Exception {
                builder.startObject();
                results.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(results.restStatus(), builder);
            }
        });
    }
}
