/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

/**
 * Base class for all action requests in Elasticsearch. An action request represents a request
 * to perform a specific operation, such as indexing a document, searching, or managing cluster state.
 *
 * <p>All concrete action requests must extend this class and implement the {@link #validate()}
 * method to ensure that the request parameters are valid before execution.
 *
 * <p>This class extends {@link AbstractTransportRequest} to support serialization and
 * deserialization for transmission across the network in a distributed Elasticsearch cluster.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Example of a concrete action request
 * public class MyCustomRequest extends ActionRequest {
 *     private String param;
 *
 *     public MyCustomRequest(String param) {
 *         this.param = param;
 *     }
 *
 *     @Override
 *     public ActionRequestValidationException validate() {
 *         ActionRequestValidationException validationException = null;
 *         if (param == null || param.isEmpty()) {
 *             validationException = ValidateActions.addValidationError(
 *                 "param must not be null or empty",
 *                 validationException
 *             );
 *         }
 *         return validationException;
 *     }
 * }
 * }</pre>
 */
public abstract class ActionRequest extends AbstractTransportRequest {

    /**
     * Constructs a new action request with default settings.
     */
    public ActionRequest() {
        super();
    }

    /**
     * Constructs a new action request by reading its state from the provided stream input.
     * This constructor is used for deserialization when receiving requests over the network.
     *
     * @param in the stream input to read the request state from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public ActionRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Validates this action request and returns any validation errors. This method is called
     * before the request is executed to ensure that all required parameters are present and valid.
     *
     * <p>Implementations should check all request parameters and accumulate validation errors
     * using {@link org.elasticsearch.action.ValidateActions#addValidationError(String, ActionRequestValidationException)}.
     *
     * @return an {@link ActionRequestValidationException} containing all validation errors,
     *         or {@code null} if the request is valid
     */
    public abstract ActionRequestValidationException validate();

    /**
     * Determines whether this task should store its result after it has finished execution.
     * Task results can be retrieved later via the Task Management API.
     *
     * <p>By default, this returns {@code false}. Subclasses can override this method to
     * enable result storage for specific request types.
     *
     * @return {@code true} if the task result should be stored, {@code false} otherwise
     */
    public boolean getShouldStoreResult() {
        return false;
    }

    /**
     * Writes this action request to the provided stream output for serialization.
     * This method is used to transmit the request across the network.
     *
     * @param out the stream output to write the request state to
     * @throws IOException if an I/O error occurs while writing to the stream
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
