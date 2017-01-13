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

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * An extension of the {@link ScheduledThreadPoolExecutor} that preserves the context of the {@link Callable} or {@link Runnable} that is
 * being scheduled for execution so that it can be restored when the execution occurs.
 */
public class EsScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private final ThreadContext threadContext;

    public EsScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler,
                                         ThreadContext threadContext) {
        super(corePoolSize, threadFactory, handler);
        this.threadContext = threadContext;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return super.schedule(threadContext.preserveContext(command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return super.schedule(threadContext.preserveContext(callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(threadContext.preserveContext(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(threadContext.preserveContext(command), initialDelay, delay, unit);
    }

    @Override
    public final void execute(final Runnable command) {
        try {
            super.execute(command);
        } catch (EsRejectedExecutionException ex) {
            if (command instanceof AbstractRunnable) {
                // If we are an abstract runnable we can handle the rejection
                // directly and don't need to rethrow it.
                try {
                    ((AbstractRunnable) command).onRejection(ex);
                } finally {
                    ((AbstractRunnable) command).onAfter();

                }
            } else {
                throw ex;
            }
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        assert assertDefaultContext(r);
    }

    private boolean assertDefaultContext(Runnable r) {
        try {
            assert threadContext.isDefaultContext() : "the thread context is not the default context and the thread [" +
                Thread.currentThread().getName() + "] is being returned to the pool after executing [" + r + "]";
        } catch (IllegalStateException ex) {
            // sometimes we execute on a closed context and isDefaultContext doesn't bypass the ensureOpen checks
            // this must not trigger an exception here since we only assert if the default is restored and
            // we don't really care if we are closed
            if (threadContext.isClosed() == false) {
                throw ex;
            }
        }
        return true;
    }
}
