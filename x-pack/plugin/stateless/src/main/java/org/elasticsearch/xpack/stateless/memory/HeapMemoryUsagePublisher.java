/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;

public class HeapMemoryUsagePublisher {

    private final NodeClient client;

    public HeapMemoryUsagePublisher(final Client client) {
        this.client = (NodeClient) client;
    }

    public void publishIndicesMappingSize(final HeapMemoryUsage heapMemoryUsage, final ActionListener<ActionResponse.Empty> listener) {
        ThreadContext threadContext = client.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            var request = new PublishHeapMemoryMetricsRequest(heapMemoryUsage);
            client.execute(TransportPublishHeapMemoryMetrics.INSTANCE, request, listener);
        }
    }
}
