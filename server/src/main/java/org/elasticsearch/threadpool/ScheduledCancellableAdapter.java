/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class ScheduledCancellableAdapter implements Scheduler.ScheduledCancellable {
    private final ScheduledFuture<?> scheduledFuture;

    ScheduledCancellableAdapter(ScheduledFuture<?> scheduledFuture) {
        assert scheduledFuture != null;
        this.scheduledFuture = scheduledFuture;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return scheduledFuture.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed other) {
        // unwrap other by calling on it.
        return -other.compareTo(scheduledFuture);
    }

    @Override
    public boolean cancel() {
        return FutureUtils.cancel(scheduledFuture);
    }

    @Override
    public boolean isCancelled() {
        return scheduledFuture.isCancelled();
    }
}
