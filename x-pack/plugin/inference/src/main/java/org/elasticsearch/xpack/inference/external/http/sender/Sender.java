/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.Closeable;

public interface Sender extends Closeable {

    /**
     * Initialize the sender synchronously. This must be called before calling {@link #send} or {@link #sendWithoutQueuing}.
     * This should only be used in testing logic.
     * @deprecated use {@link #startAsynchronously} instead.
     */
    void startSynchronously();

    /**
     * Initialize the sender asynchronously. This must be called before calling {@link #send} or {@link #sendWithoutQueuing}.
     * The listener will be notified when the initialization is complete or if it fails.
     * @param listener the listener to notify when initialization is complete or if it fails
     * @param timeout the maximum time to wait for initialization to complete. If null, the implementation should use a default timeout.
     */
    void startAsynchronously(ActionListener<Void> listener, @Nullable TimeValue timeout);

    /**
     * Send the inference request to the inference service.
     */
    void send(
        RequestManager requestCreator,
        InferenceInputs inferenceInputs,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );

    /**
     * Send the inference request to the inference service without queuing.
     * This should only be used for infrequent requests that should not be queued
     * (e.g. {@link org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceAuthorizationRequest})
     */
    void sendWithoutQueuing(
        Logger logger,
        Request request,
        ResponseHandler responseHandler,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    );
}
