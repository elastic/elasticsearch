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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Future;

/**
 * An action listener that wraps another action listener and threading its execution.
 */
public final class ThreadedActionListener<Response> implements ActionListener<Response> {

    /**
     * Wrapper that can be used to automatically wrap a listener in a threaded listener if needed.
     */
    public static class Wrapper {

        private final Logger logger;
        private final ThreadPool threadPool;

        private final boolean threadedListener;

        public Wrapper(Logger logger, Settings settings, ThreadPool threadPool) {
            this.logger = logger;
            this.threadPool = threadPool;
             // Should the action listener be threaded or not by default. Action listeners are automatically threaded for
            // the transport client in order to make sure client side code is not executed on IO threads.
            this.threadedListener = TransportClient.CLIENT_TYPE.equals(Client.CLIENT_TYPE_SETTING_S.get(settings));
        }

        public <Response> ActionListener<Response> wrap(ActionListener<Response> listener) {
            if (threadedListener == false) {
                return listener;
            }
            // if its a future, the callback is very lightweight (flipping a bit) so no need to wrap it
            if (listener instanceof Future) {
                return listener;
            }
            // already threaded...
            if (listener instanceof ThreadedActionListener) {
                return listener;
            }
            return new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.LISTENER, listener, false);
        }
    }

    private final Logger logger;
    private final ThreadPool threadPool;
    private final String executor;
    private final ActionListener<Response> listener;
    private final boolean forceExecution;

    public ThreadedActionListener(Logger logger, ThreadPool threadPool, String executor, ActionListener<Response> listener,
                                  boolean forceExecution) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.executor = executor;
        this.listener = listener;
        this.forceExecution = forceExecution;
    }

    @Override
    public void onResponse(final Response response) {
        threadPool.executor(executor).execute(new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            protected void doRun() throws Exception {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void onFailure(final Exception e) {
        threadPool.executor(executor).execute(new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            protected void doRun() throws Exception {
                listener.onFailure(e);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage("failed to execute failure callback on [{}]", listener), e);
            }
        });
    }
}
