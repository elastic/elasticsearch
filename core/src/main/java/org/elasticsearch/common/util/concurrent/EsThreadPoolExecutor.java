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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * An extension to thread pool executor, allowing (in the future) to add specific additional stats to it.
 */
public class EsThreadPoolExecutor extends ThreadPoolExecutor {

    private final ThreadContext contextHolder;
    private volatile ShutdownListener listener;

    private final Object monitor = new Object();
    /**
     * Name used in error reporting.
     */
    protected final String name;

    EsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, ThreadContext contextHolder) {
        this(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new EsAbortPolicy(), contextHolder);
    }

    EsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, XRejectedExecutionHandler handler,
            ThreadContext contextHolder) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.name = name;
        this.contextHolder = contextHolder;
    }

    public void shutdown(ShutdownListener listener) {
        synchronized (monitor) {
            if (this.listener != null) {
                throw new IllegalStateException("Shutdown was already called on this thread pool");
            }
            if (isTerminated()) {
                listener.onTerminated();
            } else {
                this.listener = listener;
            }
        }
        shutdown();
    }

    @Override
    protected synchronized void terminated() {
        super.terminated();
        synchronized (monitor) {
            if (listener != null) {
                try {
                    listener.onTerminated();
                } finally {
                    listener = null;
                }
            }
        }
    }

    public interface ShutdownListener {
        void onTerminated();
    }

    @Override
    public void execute(final Runnable command) {
        doExecute(wrapRunnable(command));
    }

    protected void doExecute(final Runnable command) {
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
            assert contextHolder.isDefaultContext() : "the thread context is not the default context and the thread [" +
                Thread.currentThread().getName() + "] is being returned to the pool after executing [" + r + "]";
        } catch (IllegalStateException ex) {
            // sometimes we execute on a closed context and isDefaultContext doen't bypass the ensureOpen checks
            // this must not trigger an exception here since we only assert if the default is restored and
            // we don't really care if we are closed
            if (contextHolder.isClosed() == false) {
                throw ex;
            }
        }
        return true;
    }

    /**
     * Returns a stream of all pending tasks. This is similar to {@link #getQueue()} but will expose the originally submitted
     * {@link Runnable} instances rather than potentially wrapped ones.
     */
    public Stream<Runnable> getTasks() {
        return this.getQueue().stream().map(this::unwrap);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append('[');
        b.append(name).append(", ");
        if (getQueue() instanceof SizeBlockingQueue) {
            @SuppressWarnings("rawtypes")
            SizeBlockingQueue queue = (SizeBlockingQueue) getQueue();
            b.append("queue capacity = ").append(queue.capacity()).append(", ");
        }
        /*
         * ThreadPoolExecutor has some nice information in its toString but we
         * can't get at it easily without just getting the toString.
         */
        b.append(super.toString()).append(']');
        return b.toString();
    }

    protected Runnable wrapRunnable(Runnable command) {
        return contextHolder.preserveContext(command);
    }

    protected Runnable unwrap(Runnable runnable) {
        return contextHolder.unwrap(runnable);
    }
}
