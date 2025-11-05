/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.core.QlClientException;

/**
 * Exception thrown for client-side errors in ESQL query processing.
 * <p>
 * This exception is used to indicate errors that originate from invalid client requests,
 * malformed queries, or incorrect usage of the ESQL API. It extends {@link QlClientException}
 * and typically maps to HTTP 400-level error responses.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Throwing a client exception for an invalid query
 * throw new EsqlClientException("Invalid query syntax at position {}", position);
 *
 * // Wrapping an underlying error
 * try {
 *     parseQuery(query);
 * } catch (ParseException e) {
 *     throw new EsqlClientException(e, "Failed to parse query: {}", query);
 * }
 * }</pre>
 */
public class EsqlClientException extends QlClientException {

    /**
     * Constructs a client exception with a formatted message.
     * <p>
     * The message can contain placeholders ({}) that will be replaced with the provided arguments.
     * </p>
     *
     * @param message the message pattern with optional placeholders
     * @param args the arguments to substitute into the message
     */
    public EsqlClientException(String message, Object... args) {
        super(message, args);
    }

    /**
     * Constructs a client exception with a message and a cause.
     *
     * @param message the detail message
     * @param cause the underlying cause of this exception
     */
    protected EsqlClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a client exception with a cause and a formatted message.
     * <p>
     * The message can contain placeholders ({}) that will be replaced with the provided arguments.
     * </p>
     *
     * @param cause the underlying cause of this exception
     * @param message the message pattern with optional placeholders
     * @param args the arguments to substitute into the message
     */
    protected EsqlClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

}
