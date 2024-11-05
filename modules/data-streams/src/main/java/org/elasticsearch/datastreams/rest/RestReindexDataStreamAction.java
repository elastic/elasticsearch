/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.ReindexDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestReindexDataStreamAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "reindex_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_reindex_data_stream"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ReindexDataStreamAction.ReindexDataStreamRequest reindexRequest = new ReindexDataStreamAction.ReindexDataStreamRequest(
            request.param("source")
        );

        // final SubscribableListener<ReindexDataStreamResponse> responseListener = new SubscribableListener<>();
        // final Task task = client.executeLocally(ReindexDataStreamAction.INSTANCE, reindexRequest, responseListener);
        //
        //
        //
        //
        // client.execute(ReindexDataStreamAction.INSTANCE, reindexRequest, responseListener);
        // responseListener.addListener(new LoggingTaskListener<>(task));
        // return sendTask(client.getLocalNodeId(), task);

        return channel -> client.execute(
            ReindexDataStreamAction.INSTANCE,
            reindexRequest,
            new RestReindexDataStreamAction.SimulateIngestRestToXContentListener(channel)
        );
    }

    private static RestChannelConsumer sendTask(String localNodeId, Task task) {
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("task", localNodeId + ":" + task.getId());
                builder.endObject();
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }

    static class SimulateIngestRestToXContentListener extends RestBuilderListener<ReindexDataStreamResponse> {

        SimulateIngestRestToXContentListener(RestChannel channel) {
            super(channel);
        }

        @Override
        public RestResponse buildResponse(ReindexDataStreamResponse response, XContentBuilder builder) throws Exception {
            assert response.isFragment() == false;
            toXContent(response, builder, channel.request());
            return new RestResponse(RestStatus.OK, builder);
        }

        private static void toXContent(ReindexDataStreamResponse response, XContentBuilder builder, ToXContent.Params params)
            throws IOException {
            builder.startObject();
            builder.field("task", response.getTaskId());
            builder.endObject();
        }
    }
}
