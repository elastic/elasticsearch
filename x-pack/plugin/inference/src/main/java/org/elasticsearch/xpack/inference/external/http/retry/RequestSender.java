/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface RequestSender {
    void send(
        Logger logger,
        OutboundRequest outboundRequest,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> listener
    );

    /**
     * Same as {@link #send(Logger, OutboundRequest, Supplier, ResponseHandler, ActionListener)}, but additionally hands every
     * outbound exchange's abort action to {@code abortHandlerRegistry}. The caller runs those actions once it gives up on the request
     * (e.g. it timed out), so that exchanges still waiting for a pooled connection or a response stop holding on to the connection
     * pool and the request body.
     * Senders that do not hold on to such resources can rely on the default implementation, which ignores the registry.
     */
    default void send(
        Logger logger,
        OutboundRequest outboundRequest,
        Supplier<Boolean> hasRequestTimedOutFunction,
        ResponseHandler responseHandler,
        Consumer<Runnable> abortHandlerRegistry,
        ActionListener<InferenceServiceResults> listener
    ) {
        send(logger, outboundRequest, hasRequestTimedOutFunction, responseHandler, listener);
    }
}
