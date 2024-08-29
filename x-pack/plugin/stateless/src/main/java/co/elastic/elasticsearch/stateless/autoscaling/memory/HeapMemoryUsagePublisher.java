/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.memory;

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
