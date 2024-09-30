/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class RestPutShutdownNodeAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "put_shutdown_node";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_nodes/{nodeId}/shutdown"));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final var parsedRequest = parseRequest(request);
        return channel -> client.execute(PutShutdownNodeAction.INSTANCE, parsedRequest, new RestToXContentListener<>(channel));
    }

    private PutShutdownNodeAction.Request parseRequest(RestRequest restRequest) throws IOException {
        try (XContentParser parser = restRequest.contentParser()) {
            return PutShutdownNodeAction.Request.parseRequest(new RestRequestFactory(restRequest), parser);
        }
    }

    private static class RestRequestFactory implements PutShutdownNodeAction.Request.Factory {
        private final String nodeId;
        private final TimeValue masterNodeTimeout;
        private final TimeValue ackTimeout;

        RestRequestFactory(RestRequest restRequest) {
            nodeId = restRequest.param("nodeId");
            masterNodeTimeout = RestUtils.getMasterNodeTimeout(restRequest);
            ackTimeout = RestUtils.getAckTimeout(restRequest);
        }

        @Override
        public PutShutdownNodeAction.Request create(
            SingleNodeShutdownMetadata.Type type,
            String reason,
            TimeValue allocationDelay,
            String targetNodeName,
            TimeValue gracePeriod
        ) {
            return new PutShutdownNodeAction.Request(
                masterNodeTimeout,
                ackTimeout,
                nodeId,
                type,
                reason,
                allocationDelay,
                targetNodeName,
                gracePeriod
            );
        }

        @Override
        public String toString() {
            return "put-shutdown-node-request-" + nodeId;
        }
    }
}
