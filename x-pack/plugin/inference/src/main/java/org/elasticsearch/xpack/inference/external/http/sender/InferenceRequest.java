/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;

/**
 * A contract for defining a request sent to a 3rd party service.
 */
public interface InferenceRequest {

    /**
     * Returns the manager that handles building and executing an inference request.
     */
    RequestManager getRequestManager();

    /**
     * Returns the inputs associated with the request.
     */
    InferenceInputs getInferenceInputs();

    /**
     * Returns the listener to notify of the results.
     */
    ActionListener<InferenceServiceResults> getListener();

    /**
     * Returns whether the request has completed. Returns true if from a failure, success, or a timeout.
     */
    boolean hasCompleted();

    /**
     * Returns the {@link TimedListener} wrapping {@link #getListener()}, which tracks the completion and timeout of the request and
     * lets senders register abort handlers for the work they start on its behalf.
     */
    TimedListener<InferenceServiceResults> getTimedListener();
}
