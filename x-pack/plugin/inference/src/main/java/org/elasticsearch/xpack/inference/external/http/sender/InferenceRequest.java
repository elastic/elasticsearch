/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;

import java.util.function.Supplier;

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
     * Returns a {@link Supplier} to determine if the request has completed.
     */
    Supplier<Boolean> getRequestCompletedFunction();
}
