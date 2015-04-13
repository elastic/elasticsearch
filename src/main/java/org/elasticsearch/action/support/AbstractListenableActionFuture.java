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

package org.elasticsearch.action.support;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

/**
 *
 */
public abstract class AbstractListenableActionFuture<T, L> extends AdapterActionFuture<T, L> implements ListenableActionFuture<T> {

    final boolean listenerThreaded;
    final ThreadPool threadPool;
    volatile Object listeners;
    boolean executedListeners = false;

    protected AbstractListenableActionFuture(boolean listenerThreaded, ThreadPool threadPool) {
        this.listenerThreaded = listenerThreaded;
        this.threadPool = threadPool;
    }

    public boolean listenerThreaded() {
        return false; // we control execution of the listener
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    @Override
    public void addListener(final ActionListener<T> listener) {
        internalAddListener(listener);
    }

    public void internalAddListener(ActionListener<T> listener) {
        boolean executeImmediate = false;
        synchronized (this) {
            if (executedListeners) {
                executeImmediate = true;
            } else {
                Object listeners = this.listeners;
                if (listeners == null) {
                    listeners = listener;
                } else if (listeners instanceof List) {
                    ((List) this.listeners).add(listener);
                } else {
                    Object orig = listeners;
                    listeners = Lists.newArrayListWithCapacity(2);
                    ((List) listeners).add(orig);
                    ((List) listeners).add(listener);
                }
                this.listeners = listeners;
            }
        }
        if (executeImmediate) {
            executeListener(listener);
        }
    }

    @Override
    protected void done() {
        super.done();
        synchronized (this) {
            executedListeners = true;
        }
        Object listeners = this.listeners;
        if (listeners != null) {
            if (listeners instanceof List) {
                List list = (List) listeners;
                for (Object listener : list) {
                    executeListener((ActionListener<T>) listener);
                }
            } else {
                executeListener((ActionListener<T>) listeners);
            }
        }
    }

    private void executeListener(final ActionListener<T> listener) {
        if (listenerThreaded) {
            try {
                threadPool.executor(ThreadPool.Names.LISTENER).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            listener.onResponse(actionGet());
                        } catch (ElasticsearchException e) {
                            listener.onFailure(e);
                        }
                    }
                });
            } catch (EsRejectedExecutionException e) {
                listener.onFailure(e);
            }
        } else {
            try {
                listener.onResponse(actionGet());
            } catch (Throwable e) {
                listener.onFailure(e);
            }
        }
    }
}