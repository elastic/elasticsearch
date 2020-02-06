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

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

public class ListenerTimeouts {

    /**
     * Wraps a listener with a listener that can timeout. After the timeout period the
     * {@link ActionListener#onFailure(Exception)} will be called with a
     * {@link ElasticsearchTimeoutException} if the listener has not already been completed.
     *
     * @param threadPool used to schedule the timeout
     * @param listener to that can timeout
     * @param timeout period before listener failed
     * @param executor to use for scheduling timeout
     * @param listenerName name of the listener for timeout exception
     * @return the wrapped listener that will timeout
     */
    public static <Response> ActionListener<Response> wrapWithTimeout(ThreadPool threadPool,  ActionListener<Response> listener,
                                                                      TimeValue timeout, String executor, String listenerName) {
        TimeoutableListener<Response> wrappedListener = new TimeoutableListener<>(listener, timeout, listenerName);
        wrappedListener.cancellable = threadPool.schedule(wrappedListener, timeout, executor);
        return wrappedListener;
    }

    private static class TimeoutableListener<Response> implements ActionListener<Response>, Runnable {

        private final AtomicBoolean isDone = new AtomicBoolean(false);
        private final ActionListener<Response> delegate;
        private final TimeValue timeout;
        private final String listenerName;
        private volatile Scheduler.ScheduledCancellable cancellable;

        private TimeoutableListener(ActionListener<Response> delegate, TimeValue timeout, String listenerName) {
            this.delegate = delegate;
            this.timeout = timeout;
            this.listenerName = listenerName;
        }

        @Override
        public void onResponse(Response response) {
            if (isDone.compareAndSet(false, true)) {
                cancellable.cancel();
                delegate.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (isDone.compareAndSet(false, true)) {
                cancellable.cancel();
                delegate.onFailure(e);
            }
        }

        @Override
        public void run() {
            if (isDone.compareAndSet(false, true)) {
                String timeoutMessage = "[" + listenerName + "]" + " timed out after [" + timeout + "]";
                delegate.onFailure(new ElasticsearchTimeoutException(timeoutMessage));
            }
        }
    }
}
