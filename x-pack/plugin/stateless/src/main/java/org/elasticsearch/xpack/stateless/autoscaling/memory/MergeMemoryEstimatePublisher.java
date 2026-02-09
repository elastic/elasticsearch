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

package org.elasticsearch.xpack.stateless.autoscaling.memory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

public class MergeMemoryEstimatePublisher {
    private static final Logger LOGGER = LogManager.getLogger(MergeMemoryEstimatePublisher.class);

    private final ThreadPool threadPool;
    private final Client client;

    public MergeMemoryEstimatePublisher(ThreadPool threadPool, Client client) {
        this.threadPool = threadPool;
        this.client = client;
    }

    public void publish(TransportPublishMergeMemoryEstimate.Request request) {
        threadPool.generic().execute(() -> {
            ThreadContext threadContext = client.threadPool().getThreadContext();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                final var failureLoggingActionListener = ActionListener.<ActionResponse.Empty>noop()
                    .delegateResponse((l, e) -> LOGGER.warn("failed to publish shard merge memory estimate", e));
                client.execute(TransportPublishMergeMemoryEstimate.INSTANCE, request, failureLoggingActionListener);
            }
        });
    }
}
