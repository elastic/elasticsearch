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

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;

/**
 *
 */
public abstract class TransportAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    protected final ThreadPool threadPool;
    protected final String actionName;
    private final ActionFilter[] filters;

    protected TransportAction(Settings settings, String actionName, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings);
        this.actionName = actionName;
        this.filters = actionFilters.filters();
        this.threadPool = threadPool;
    }

    public final ActionFuture<Response> execute(Request request) throws ElasticsearchException {
        PlainActionFuture<Response> future = newFuture();
        // since we don't have a listener, and we release a possible lock with the future
        // there is no need to execute it under a listener thread
        request.listenerThreaded(false);
        execute(request, future);
        return future;
    }

    public final void execute(Request request, ActionListener<Response> listener) {
        if (forceThreadedListener()) {
            request.listenerThreaded(true);
        }
        if (request.listenerThreaded()) {
            listener = new ThreadedActionListener<>(threadPool, listener, logger);
        }

        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }

        if (filters.length == 0) {
            try {
                doExecute(request, listener);
            } catch(Throwable t) {
                logger.trace("Error during transport action execution.", t);
                listener.onFailure(t);
            }
        } else {
            RequestFilterChain requestFilterChain = new RequestFilterChain<>(this, logger);
            requestFilterChain.proceed(actionName, request, listener);
        }
    }

    protected boolean forceThreadedListener() {
        return false;
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
                threadPool.executor(ThreadPool.Names.LISTENER).execute(new Runnable() {
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
                logger.debug("Can not run threaded action, execution rejected [{}] running on current thread", listener);
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
                threadPool.executor(ThreadPool.Names.LISTENER).execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onFailure(e);
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not run threaded action, execution rejected for listener [{}] running on current thread", listener);
                /* we don't care if that takes long since we are shutting down (or queue capacity). But if we not respond somebody could wait
                 * for the response on the listener side which could be a remote machine so make sure we push it out there.*/
                listener.onFailure(e);
            }
        }
    }

    private static class RequestFilterChain<Request extends ActionRequest, Response extends ActionResponse> implements ActionFilterChain {

        private final TransportAction<Request, Response> action;
        private final AtomicInteger index = new AtomicInteger();
        private final ESLogger logger;

        private RequestFilterChain(TransportAction<Request, Response> action, ESLogger logger) {
            this.action = action;
            this.logger = logger;
        }

        @Override @SuppressWarnings("unchecked")
        public void proceed(String actionName, ActionRequest request, ActionListener listener) {
            int i = index.getAndIncrement();
            try {
                if (i < this.action.filters.length) {
                    this.action.filters[i].apply(actionName, request, listener, this);
                } else if (i == this.action.filters.length) {
                    this.action.doExecute((Request) request, new FilteredActionListener<Response>(actionName, listener, new ResponseFilterChain(this.action.filters, logger)));
                } else {
                    listener.onFailure(new IllegalStateException("proceed was called too many times"));
                }
            } catch(Throwable t) {
                logger.trace("Error during transport action execution.", t);
                listener.onFailure(t);
            }
        }

        @Override
        public void proceed(String action, ActionResponse response, ActionListener listener) {
            assert false : "request filter chain should never be called on the response side";
        }
    }

    private static class ResponseFilterChain implements ActionFilterChain {

        private final ActionFilter[] filters;
        private final AtomicInteger index;
        private final ESLogger logger;

        private ResponseFilterChain(ActionFilter[] filters, ESLogger logger) {
            this.filters = filters;
            this.index = new AtomicInteger(filters.length);
            this.logger = logger;
        }

        @Override
        public void proceed(String action, ActionRequest request, ActionListener listener) {
            assert false : "response filter chain should never be called on the request side";
        }

        @Override @SuppressWarnings("unchecked")
        public void proceed(String action, ActionResponse response, ActionListener listener) {
            int i = index.decrementAndGet();
            try {
                if (i >= 0) {
                    filters[i].apply(action, response, listener, this);
                } else if (i == -1) {
                    listener.onResponse(response);
                } else {
                    listener.onFailure(new IllegalStateException("proceed was called too many times"));
                }
            } catch (Throwable t) {
                logger.trace("Error during transport action execution.", t);
                listener.onFailure(t);
            }
        }
    }

    private static class FilteredActionListener<Response extends ActionResponse> implements ActionListener<Response> {

        private final String actionName;
        private final ActionListener listener;
        private final ResponseFilterChain chain;

        private FilteredActionListener(String actionName, ActionListener listener, ResponseFilterChain chain) {
            this.actionName = actionName;
            this.listener = listener;
            this.chain = chain;
        }

        @Override
        public void onResponse(Response response) {
            chain.proceed(actionName, response, listener);
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(e);
        }
    }
}
