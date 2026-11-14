/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.inference.external.http.sender.TimedListener;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;

public interface RequestSender {
    /**
     * Sends the request and notifies {@code timedListener} of the result.
     * <p>
     * The timed listener tells whether the request already completed (e.g. timed out), in which case nothing should be sent anymore.
     * Senders holding on to resources for an outbound exchange (e.g. a request waiting for a pooled connection) must register an
     * {@link TimedListener#addAbortHandler(Runnable) abort handler} releasing them, because failing the listener on a timeout does
     * not stop the exchange by itself.
     */
    void send(
        Logger logger,
        OutboundRequest outboundRequest,
        ResponseHandler responseHandler,
        TimedListener<InferenceServiceResults> timedListener
    );
}
