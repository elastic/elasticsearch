/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.function.Consumer;

class RetryListener extends ActionListener.Delegating<ScrollableHitSource.Response, ScrollableHitSource.Response>
    implements
        RejectAwareActionListener<ScrollableHitSource.Response> {
    private final Logger logger;
    private final Iterator<TimeValue> retries;
    private final ThreadPool threadPool;
    private final Consumer<RejectAwareActionListener<ScrollableHitSource.Response>> retryScrollHandler;
    private int retryCount = 0;

    RetryListener(
        Logger logger,
        ThreadPool threadPool,
        BackoffPolicy backoffPolicy,
        Consumer<RejectAwareActionListener<ScrollableHitSource.Response>> retryScrollHandler,
        ActionListener<ScrollableHitSource.Response> delegate
    ) {
        super(delegate);
        this.logger = logger;
        this.threadPool = threadPool;
        this.retries = backoffPolicy.iterator();
        this.retryScrollHandler = retryScrollHandler;
    }

    @Override
    public void onResponse(ScrollableHitSource.Response response) {
        delegate.onResponse(response);
    }

    @Override
    public void onRejection(Exception e) {
        if (retries.hasNext()) {
            retryCount += 1;
            TimeValue delay = retries.next();
            logger.trace(() -> new ParameterizedMessage("retrying rejected search after [{}]", delay), e);
            schedule(() -> retryScrollHandler.accept(this), delay);
        } else {
            logger.warn(() -> new ParameterizedMessage("giving up on search because we retried [{}] times without success", retryCount), e);
            delegate.onFailure(e);
        }
    }

    private void schedule(Runnable runnable, TimeValue delay) {
        threadPool.schedule(runnable, delay, ThreadPool.Names.SAME);
    }
}
