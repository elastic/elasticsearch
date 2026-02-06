/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.function.Consumer;

class RetryListener extends DelegatingActionListener<HitSource.Response, HitSource.Response>
    implements
        RejectAwareActionListener<HitSource.Response> {
    private final Logger logger;
    private final Iterator<TimeValue> retries;
    private final ThreadPool threadPool;
    private final Consumer<RejectAwareActionListener<HitSource.Response>> retryScrollHandler;
    private int retryCount = 0;

    RetryListener(
        Logger logger,
        ThreadPool threadPool,
        BackoffPolicy backoffPolicy,
        Consumer<RejectAwareActionListener<HitSource.Response>> retryScrollHandler,
        ActionListener<HitSource.Response> delegate
    ) {
        super(delegate);
        this.logger = logger;
        this.threadPool = threadPool;
        this.retries = backoffPolicy.iterator();
        this.retryScrollHandler = retryScrollHandler;
    }

    @Override
    public void onResponse(HitSource.Response response) {
        delegate.onResponse(response);
    }

    @Override
    public void onRejection(Exception e) {
        if (retries.hasNext()) {
            retryCount += 1;
            TimeValue delay = retries.next();
            logger.trace(() -> "retrying rejected search after [" + delay + "]", e);
            schedule(() -> retryScrollHandler.accept(this), delay);
        } else {
            logger.warn(() -> "giving up on search because we retried [" + retryCount + "] times without success", e);
            delegate.onFailure(e);
        }
    }

    private void schedule(Runnable runnable, TimeValue delay) {
        threadPool.schedule(runnable, delay, EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }
}
