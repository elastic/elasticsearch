/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.rest;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMigrationReindexAction extends BaseRestHandler {
    public static final String MIGRATION_REINDEX_CAPABILITY = "migration_reindex";

    @Override
    public String getName() {
        return "migration_reindex";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_migration/reindex"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ReindexDataStreamAction.ReindexDataStreamRequest reindexRequest;
        try (XContentParser parser = request.contentParser()) {
            reindexRequest = ReindexDataStreamAction.ReindexDataStreamRequest.fromXContent(parser);
        }
        return channel -> client.execute(
            ReindexDataStreamAction.INSTANCE,
            reindexRequest,
            new ReindexDataStreamRestToXContentListener(channel)
        );
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(MIGRATION_REINDEX_CAPABILITY);
    }

    static class ReindexDataStreamRestToXContentListener extends RestBuilderListener<AcknowledgedResponse> {

        ReindexDataStreamRestToXContentListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(AcknowledgedResponse response, XContentBuilder builder) throws Exception {
            response.toXContent(builder, channel.request());
            return new RestResponse(RestStatus.OK, builder);
        }
    }
}
