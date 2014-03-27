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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
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

    public ActionFuture<Response> execute(Request request) throws ElasticsearchException {
        PlainActionFuture<Response> future = newFuture();
        // since we don't have a listener, and we release a possible lock with the future
        // there is no need to execute it under a listener thread
        request.listenerThreaded(false);
        execute(request, future);
        return future;
    }

    public void execute(Request request, ActionListener<Response> listener) {
        if (request.listenerThreaded()) {
            listener = new ThreadedActionListener<>(threadPool, listener, logger);
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

    static final class ThreadedActionListener<Response> implements ActionListener<Response> {

        private final ThreadPool threadPool;

        private final ActionListener<Response> listener;

        private final ESLogger logger;

        ThreadedActionListener(ThreadPool threadPool, ActionListener<Response> listener, ESLogger logger) {
            this.threadPool = threadPool;
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public void onResponse(final Response response) {
            try {
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
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not run threaded action, exectuion rejected [{}] running on current thread", listener);
                /* we don't care if that takes long since we are shutting down. But if we not respond somebody could wait
                 * for the response on the listener side which could be a remote machine so make sure we push it out there.*/
                try {
                    listener.onResponse(response);
                } catch (Throwable e) {
                    listener.onFailure(e);
                }
            }
        }
        
        @Override
        public void onFailure(final Throwable e) {
            try {
                threadPool.generic().execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onFailure(e);
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not run threaded action, exectuion rejected for listener [{}] running on current thread", listener);
                /* we don't care if that takes long since we are shutting down. But if we not respond somebody could wait
                 * for the response on the listener side which could be a remote machine so make sure we push it out there.*/
                listener.onFailure(e);
            }
        }
    }
}
