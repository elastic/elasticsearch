/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class AllocationActionListener<T> {

    private final ActionListener<T> delegate;
    private final SetOnce<T> response = new SetOnce<>();
    private final AtomicInteger listenersExecuted = new AtomicInteger(2);
    private final ThreadContext context;
    private final Supplier<ThreadContext.StoredContext> original;
    private final SetOnce<Map<String, List<String>>> additionalResponseHeaders = new SetOnce<>();

    /**
     * Returns a no-op listener for callers that do not need to wait for reroute to finish (e.g. shard rebalancing across the cluster)
     * before completing their own operation.
     *
     * For example, waiting for reroute is needed to allocate shards for a newly created index, but not when deleting one.
     */
    public static ActionListener<Void> rerouteCompletionIsNotRequired() {
        return ActionListener.noop();
    }

    public AllocationActionListener(ActionListener<T> delegate, ThreadContext context) {
        this.delegate = delegate;
        this.context = context;
        this.original = context.newRestorableContext(false);
    }

    private void notifyListenerExecuted() {
        if (listenersExecuted.decrementAndGet() == 0) {
            executeInContext(() -> delegate.onResponse(AllocationActionListener.this.response.get()));
        }
    }

    private void notifyListenerFailed(Exception e) {
        executeInContext(() -> delegate.onFailure(e));
    }

    private void executeInContext(Runnable action) {
        try (ThreadContext.StoredContext ignore2 = original.get()) {
            appendAdditionalResponseHeaders(context, additionalResponseHeaders.get());
            action.run();
        }
    }

    private static void appendAdditionalResponseHeaders(ThreadContext context, Map<String, List<String>> additionalHeaders) {
        if (additionalHeaders != null) {
            for (var entry : additionalHeaders.entrySet()) {
                for (String header : entry.getValue()) {
                    context.addResponseHeader(entry.getKey(), header);
                }
            }
        }
    }

    public ActionListener<T> clusterStateUpdate() {
        return new ActionListener<>() {
            @Override
            public void onResponse(T response) {
                AllocationActionListener.this.response.set(response);
                additionalResponseHeaders.set(context.getResponseHeaders());
                notifyListenerExecuted();
            }

            @Override
            public void onFailure(Exception e) {
                additionalResponseHeaders.set(context.getResponseHeaders());
                notifyListenerFailed(e);
            }
        };
    }

    public ActionListener<Void> reroute() {
        return new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                notifyListenerExecuted();
            }

            @Override
            public void onFailure(Exception e) {
                notifyListenerFailed(e);
            }
        };
    }
}
