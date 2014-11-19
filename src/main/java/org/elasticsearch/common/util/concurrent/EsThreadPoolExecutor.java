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

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionRunnable;

import java.util.concurrent.*;

/**
 * An extension to thread pool executor, allowing (in the future) to add specific additional stats to it.
 */
public class EsThreadPoolExecutor extends ThreadPoolExecutor {

    private volatile ShutdownListener listener;

    private final Object monitor = new Object();

    EsThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new EsAbortPolicy());
    }

    EsThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, XRejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public void shutdown(ShutdownListener listener) {
        synchronized (monitor) {
            if (this.listener != null) {
                throw new ElasticsearchIllegalStateException("Shutdown was already called on this thread pool");
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

    public static interface ShutdownListener {
        public void onTerminated();
    }

    @Override
    public void execute(Runnable command) {
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
}
