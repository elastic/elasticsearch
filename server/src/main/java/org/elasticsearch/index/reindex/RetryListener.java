/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.function.Consumer;

class RetryListener implements RejectAwareActionListener<ScrollableHitSource.Response> {
    private final Logger logger;
    private final Iterator<TimeValue> retries;
    private final ThreadPool threadPool;
    private final Consumer<RejectAwareActionListener<ScrollableHitSource.Response>> retryScrollHandler;
    private final ActionListener<ScrollableHitSource.Response> delegate;
    private int retryCount = 0;

    RetryListener(Logger logger, ThreadPool threadPool, BackoffPolicy backoffPolicy,
                          Consumer<RejectAwareActionListener<ScrollableHitSource.Response>> retryScrollHandler,
                          ActionListener<ScrollableHitSource.Response> delegate) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.retries = backoffPolicy.iterator();
        this.retryScrollHandler = retryScrollHandler;
        this.delegate = delegate;
    }

    @Override
    public void onResponse(ScrollableHitSource.Response response) {
        delegate.onResponse(response);
    }

    @Override
    public void onFailure(Exception e) {
        delegate.onFailure(e);
    }

    @Override
    public void onRejection(Exception e) {
        if (retries.hasNext()) {
            retryCount += 1;
            TimeValue delay = retries.next();
            logger.trace(() -> new ParameterizedMessage("retrying rejected search after [{}]", delay), e);
            schedule(() -> retryScrollHandler.accept(this), delay);
        } else {
            logger.warn(() -> new ParameterizedMessage(
                "giving up on search because we retried [{}] times without success", retryCount), e);
            delegate.onFailure(e);
        }
    }

    private void schedule(Runnable runnable, TimeValue delay) {
        threadPool.schedule(runnable, delay, ThreadPool.Names.SAME);
    }
}
