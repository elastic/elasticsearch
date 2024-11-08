/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.ratelimit.RateLimitable;

import java.util.function.Supplier;

/**
 * A contract for constructing a {@link Runnable} to handle sending an inference request to a 3rd party service.
 */
public interface RequestManager extends RateLimitable {
    void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    );

    // TODO For batching we'll add 2 new method: prepare(query, input, ...) which will allow the individual
    // managers to implement their own batching
    // executePreparedRequest() which will execute all prepared requests aka sends the batch

    String inferenceEntityId();
}
