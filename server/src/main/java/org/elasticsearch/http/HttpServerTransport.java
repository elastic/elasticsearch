/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface HttpServerTransport extends LifecycleComponent, ReportingService<HttpInfo> {

    Logger logger = LogManager.getLogger(HttpServerTransport.class);

    String HTTP_PROFILE_NAME = ".http";
    String HTTP_SERVER_WORKER_THREAD_NAME_PREFIX = "http_server_worker";

    BoundTransportAddress boundAddress();

    @Override
    HttpInfo info();

    HttpStats stats();

    /**
     * Dispatches HTTP requests.
     */
    interface Dispatcher {

        /**
         * Dispatches the {@link RestRequest} to the relevant request handler or responds to the given rest channel directly if
         * the request can't be handled by any request handler.
         *
         * @param request       the request to dispatch
         * @param channel       the response channel of this request
         * @param threadContext the thread context
         */
        void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext);

        /**
         * Dispatches a bad request. For example, if a request is malformed it will be dispatched via this method with the cause of the bad
         * request.
         *
         * @param channel       the response channel of this request
         * @param threadContext the thread context
         * @param cause         the cause of the bad request
         */
        void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause);

        static Dispatcher dispatchWithThreadContextWrapper(Dispatcher dispatcher, Consumer<HttpPreRequest> setDispatcherContext) {
            return dispatchWithThreadContextWrapper(
                dispatcher,
                (restRequest, threadContext) -> setDispatcherContext.accept(restRequest.getHttpRequest())
            );
        };

        static Dispatcher dispatchWithThreadContextWrapper(
            Dispatcher dispatcher,
            BiConsumer<RestRequest, ThreadContext> setDispatcherContext
        ) {
            return new Dispatcher() {
                @Override
                public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                    populateRequestThreadContext(request, channel, threadContext);
                    dispatcher.dispatchRequest(request, channel, threadContext);
                }

                @Override
                public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                    dispatcher.dispatchBadRequest(channel, threadContext, cause);
                }

                private void populateRequestThreadContext(RestRequest restRequest, RestChannel channel, ThreadContext threadContext) {
                    try {
                        setDispatcherContext.accept(restRequest, threadContext);
                    } catch (Exception e) {
                        try {
                            channel.sendResponse(new RestResponse(channel, e));
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.error(() -> "failed to send failure response for uri [" + restRequest.uri() + "]", inner);
                        }
                    }
                }
            };
        }
    }
}
