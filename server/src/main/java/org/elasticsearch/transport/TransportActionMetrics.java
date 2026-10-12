/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A {@link TransportMessageListener} that records the sizes in bytes of the transport action requests and responses that this node sends
 * and receives as OTel histograms. Uses same bucket boundaries as {@link TransportActionStats}.
 */
public final class TransportActionMetrics implements TransportMessageListener {

    public static final String SENT_REQUEST_SIZE_METRIC = "es.transport.sent.request.size";
    public static final String RECEIVED_REQUEST_SIZE_METRIC = "es.transport.received.request.size";
    public static final String SENT_RESPONSE_SIZE_METRIC = "es.transport.sent.response.size";
    public static final String RECEIVED_RESPONSE_SIZE_METRIC = "es.transport.received.response.size";

    public static final String ACTION_NAME_ATTRIBUTE = "es.transport.action.name";
    public static final String IS_REMOTE_CONNECTION_ATTRIBUTE = "es.transport.connection.is_remote";

    private static final List<Long> BUCKET_BOUNDARIES = Arrays.stream(TransportActionStatsTracker.getBucketUpperBounds())
        .asLongStream()
        .boxed()
        .toList();

    private final LongHistogram sentRequestSize;
    private final LongHistogram receivedRequestSize;
    private final LongHistogram sentResponseSize;
    private final LongHistogram receivedResponseSize;

    public TransportActionMetrics(MeterRegistry meterRegistry) {
        this.sentRequestSize = meterRegistry.registerLongHistogram(
            SENT_REQUEST_SIZE_METRIC,
            "Size, in bytes, of the transport action requests sent by this node",
            "By",
            BUCKET_BOUNDARIES
        );
        this.receivedRequestSize = meterRegistry.registerLongHistogram(
            RECEIVED_REQUEST_SIZE_METRIC,
            "Size, in bytes, of the transport action requests received by this node",
            "By",
            BUCKET_BOUNDARIES
        );
        this.sentResponseSize = meterRegistry.registerLongHistogram(
            SENT_RESPONSE_SIZE_METRIC,
            "Size, in bytes, of the transport action responses sent by this node",
            "By",
            BUCKET_BOUNDARIES
        );
        this.receivedResponseSize = meterRegistry.registerLongHistogram(
            RECEIVED_RESPONSE_SIZE_METRIC,
            "Size, in bytes, of the transport action responses received by this node",
            "By",
            BUCKET_BOUNDARIES
        );
    }

    @Override
    public void onRequestReceived(long requestId, String action, int networkMessageSize) {
        record(receivedRequestSize, action, networkMessageSize, false);
    }

    @Override
    public void onResponseSent(long requestId, String action, int networkMessageSize) {
        record(sentResponseSize, action, networkMessageSize, false);
    }

    @Override
    public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
        onResponseSent(requestId, action, networkMessageSize);
    }

    @Override
    public void onRequestSent(
        DiscoveryNode node,
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions finalOptions,
        int networkMessageSize
    ) {
        record(sentRequestSize, action, networkMessageSize, false);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void onResponseReceived(long requestId, @Nullable Transport.ResponseContext context, int networkMessageSize) {
        if (networkMessageSize >= 0 && context != null) {
            record(receivedResponseSize, context.action(), networkMessageSize, isRemoteConnection(context.connection()));
        }
    }

    private static boolean isRemoteConnection(Transport.Connection connection) {
        return RemoteConnectionManager.resolveRemoteClusterProjectInfo(connection).isPresent();
    }

    private static void record(LongHistogram histogram, String action, long bytes, boolean isRemoteConnection) {
        if (bytes < 0) {
            return;
        }
        final Map<String, Object> attributes = isRemoteConnection
            ? Map.of(ACTION_NAME_ATTRIBUTE, action, IS_REMOTE_CONNECTION_ATTRIBUTE, true)
            : Map.of(ACTION_NAME_ATTRIBUTE, action);
        histogram.record(bytes, attributes);
    }
}
