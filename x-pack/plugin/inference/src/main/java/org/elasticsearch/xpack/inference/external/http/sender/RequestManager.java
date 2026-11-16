/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.ratelimit.RateLimitable;

/**
 * A contract for constructing a {@link Runnable} to handle sending an inference request to a 3rd party service.
 */
public interface RequestManager extends RateLimitable {
    /**
     * Builds the request for the inputs and sends it with the request sender.
     *
     * @param timedListener notified of the result. It also tells whether the request already completed (e.g. timed out), and lets
     *                      the sender register actions aborting work that is abandoned once the request fails.
     */
    void execute(InferenceInputs inferenceInputs, RequestSender requestSender, TimedListener<InferenceServiceResults> timedListener);

    // TODO For batching we'll add 2 new method: prepare(query, input, ...) which will allow the individual
    // managers to implement their own batching
    // executePreparedRequest() which will execute all prepared requests aka sends the batch

    String inferenceEntityId();

    // TODO: add service() and taskType()
}
