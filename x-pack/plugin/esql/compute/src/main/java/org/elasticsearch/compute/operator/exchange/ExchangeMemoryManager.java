/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.operator.Operator;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allows bounding the number of in-flight pages in {@link PassthroughExchanger}
 */
@Experimental
public class ExchangeMemoryManager {
    private final int bufferMaxPages;

    private final AtomicInteger bufferedPages = new AtomicInteger();
    private ListenableActionFuture<Void> notFullFuture;

    public ExchangeMemoryManager(int bufferMaxPages) {
        this.bufferMaxPages = bufferMaxPages;
    }

    public void addPage() {
        bufferedPages.incrementAndGet();
    }

    public void releasePage() {
        int pages = bufferedPages.decrementAndGet();
        if (pages <= bufferMaxPages && (pages + 1) > bufferMaxPages) {
            ListenableActionFuture<Void> future;
            synchronized (this) {
                // if we have no callback waiting, return early
                if (notFullFuture == null) {
                    return;
                }
                future = notFullFuture;
                notFullFuture = null;
            }
            // complete future outside of lock since this can invoke callbacks
            future.onResponse(null);
        }
    }

    public ListenableActionFuture<Void> getNotFullFuture() {
        if (bufferedPages.get() <= bufferMaxPages) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (this) {
            // Recheck after synchronizing but before creating a real listener
            if (bufferedPages.get() <= bufferMaxPages) {
                return Operator.NOT_BLOCKED;
            }
            // if we are full and no current listener is registered, create one
            if (notFullFuture == null) {
                notFullFuture = new ListenableActionFuture<>();
            }
            return notFullFuture;
        }
    }
}
