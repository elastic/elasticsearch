/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;

import java.util.Collection;

/**
 * Exception thrown when ESQL query verification fails.
 * <p>
 * This exception is raised during the verification phase of query processing when the query
 * contains semantic errors, invalid operations, or constraint violations. It extends
 * {@link EsqlClientException} and typically includes detailed failure information to help
 * users diagnose and fix their queries.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Throwing a verification exception with a simple message
 * throw new VerificationException("Column {} does not exist", columnName);
 *
 * // Creating from a collection of failures
 * Collection<Failure> failures = new ArrayList<>();
 * failures.add(Failure.fail(node, "Type mismatch: expected {}, got {}", expectedType, actualType));
 * throw new VerificationException(failures);
 *
 * // Creating from a Failures object
 * Failures failures = new Failures();
 * failures.add(Failure.fail(node, "Invalid operation"));
 * throw new VerificationException(failures);
 * }</pre>
 */
public class VerificationException extends EsqlClientException {
    /**
     * Constructs a verification exception with a formatted message.
     * <p>
     * The message can contain placeholders ({}) that will be replaced with the provided arguments.
     * </p>
     *
     * @param message the message pattern with optional placeholders
     * @param args the arguments to substitute into the message
     */
    public VerificationException(String message, Object... args) {
        super(message, args);
    }

    /**
     * Constructs a verification exception from a collection of failures.
     * <p>
     * The exception message will be automatically formatted to include all failures
     * with their line numbers and column positions.
     * </p>
     *
     * @param sources the collection of failures that caused this exception
     */
    public VerificationException(Collection<Failure> sources) {
        super(Failure.failMessage(sources));
    }

    /**
     * Constructs a verification exception from a Failures object.
     * <p>
     * The exception message will be derived from the string representation of the Failures object.
     * </p>
     *
     * @param failures the failures object containing all verification failures
     */
    public VerificationException(Failures failures) {
        super(failures.toString());
    }

    /**
     * Constructs a verification exception with a message and a cause.
     *
     * @param message the detail message
     * @param cause the underlying cause of this exception
     */
    public VerificationException(String message, Throwable cause) {
        super(message, cause);
    }

}
