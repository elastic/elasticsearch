/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.TaskType;

import java.net.URI;

public interface OutboundRequest {
    void createHttpRequest(ActionListener<HttpRequest> listener);

    URI getURI();

    /**
     * Create a new request with less input text.
     * @return a new {@link OutboundRequest} with the truncated input text
     */
    OutboundRequest truncate();

    /**
     * Returns an array of booleans indicating if the text input at that same array index was truncated in the request
     * sent to the 3rd party server.
     */
    boolean[] getTruncationInfo();

    /**
     * Provides access to an identifier to determine which inference configuration this request originated from.
     * @return the unique identifier for the inference entity configuration
     */
    String getInferenceEntityId();

    /**
     * @return the {@link TaskType} of the request
     */
    TaskType getTaskType();

    /**
     * Streams the result in bytes to the {@link org.elasticsearch.inference.InferenceServiceResults}.
     * Defaults to false.
     */
    default boolean isStreaming() {
        return false;
    }
}
