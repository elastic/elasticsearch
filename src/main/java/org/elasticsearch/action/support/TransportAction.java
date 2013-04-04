/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;

/**
 *
 */
public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    protected final ThreadPool threadPool;

    protected TransportAction(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;
    }

    public ActionFuture<Response> execute(Request request) throws ElasticSearchException {
        PlainActionFuture<Response> future = newFuture();
        // since we don't have a listener, and we release a possible lock with the future
        // there is no need to execute it under a listener thread
        request.listenerThreaded(false);
        execute(request, future);
        return future;
    }

    public void execute(Request request, ActionListener<Response> listener) {
        if (request.listenerThreaded()) {
            listener = new ThreadedActionListener<Response>(threadPool, listener);
        }
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        try {
            doExecute(request, listener);
        } catch (Throwable e) {
            logger.trace("Error during transport action execution.", e);
            listener.onFailure(e);
        }
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);

    static class ThreadedActionListener<Response> implements ActionListener<Response> {

        private final ThreadPool threadPool;

        private final ActionListener<Response> listener;

        ThreadedActionListener(ThreadPool threadPool, ActionListener<Response> listener) {
            this.threadPool = threadPool;
            this.listener = listener;
        }

        @Override
        public void onResponse(final Response response) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        listener.onResponse(response);
                    } catch (Throwable e) {
                        listener.onFailure(e);
                    }
                }
            });
        }

        @Override
        public void onFailure(final Throwable e) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    listener.onFailure(e);
                }
            });
        }
    }
}
