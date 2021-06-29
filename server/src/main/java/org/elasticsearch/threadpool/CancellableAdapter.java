/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.Future;

class CancellableAdapter implements Scheduler.Cancellable {
    private Future<?> future;

    CancellableAdapter(Future<?> future) {
        assert future != null;
        this.future = future;
    }

    @Override
    public boolean cancel() {
        return FutureUtils.cancel(future);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }
}
