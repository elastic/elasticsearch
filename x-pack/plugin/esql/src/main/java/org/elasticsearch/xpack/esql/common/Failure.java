/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common;

import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Represents a failure associated with a specific node in the ESQL query tree.
 * <p>
 * Failures are typically used during query verification to track semantic errors,
 * type mismatches, or other validation issues. Each failure is attached to the
 * specific node in the query tree where the problem was detected.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Creating a failure for a node with a formatted message
 * Failure failure = Failure.fail(node, "Column {} not found in table {}", columnName, tableName);
 *
 * // Creating a failure directly
 * Failure failure = new Failure(node, "Invalid operation on this node");
 *
 * // Collecting failures during verification
 * Collection<Failure> failures = new ArrayList<>();
 * if (node.hasError()) {
 *     failures.add(Failure.fail(node, "Error: {}", node.getError()));
 * }
 *
 * // Getting a formatted message from multiple failures
 * String errorMessage = Failure.failMessage(failures);
 * }</pre>
 */
public class Failure {

    private final Node<?> node;
    private final String message;

    /**
     * Constructs a failure associated with a specific node.
     *
     * @param node the query tree node where the failure occurred
     * @param message the failure message describing the problem
     */
    public Failure(Node<?> node, String message) {
        this.node = node;
        this.message = message;
    }

    /**
     * Returns the node associated with this failure.
     *
     * @return the query tree node where the failure occurred
     */
    public Node<?> node() {
        return node;
    }

    /**
     * Returns the failure message.
     *
     * @return the message describing the failure
     */
    public String message() {
        return message;
    }

    @Override
    public int hashCode() {
        if (node instanceof UnresolvedAttribute ua) {
            return ua.hashCode(true);
        }
        return Objects.hash(node);
    }

    /**
     * Equality is based on the contained node, the failure is "attached" to it.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Failure other = (Failure) obj;

        // When deduplicating failures, it's important to ignore the NameIds of UnresolvedAttributes.
        // Otherwise, two failures will be emitted to the user for e.g.
        // `FROM test | STATS max(unknown) by unknown` because the two `unknown` attributes will have differentNameIds - even though they
        // clearly refer to the same problem.
        if (node instanceof UnresolvedAttribute ua) {
            return ua.equals(other.node, true);
        }
        return Objects.equals(node, other.node);
    }

    /**
     * Returns the string representation of this failure.
     * <p>
     * This returns only the message part, without location information.
     * Use {@link #failMessage(Collection)} to get formatted messages with locations.
     * </p>
     *
     * @return the failure message
     */
    @Override
    public String toString() {
        return message;
    }

    /**
     * Creates a failure with a formatted message.
     * <p>
     * This is a convenience factory method that formats the message using the provided
     * arguments before creating the Failure instance. Message placeholders are denoted by {}.
     * </p>
     *
     * @param source the query tree node where the failure occurred
     * @param message the message pattern with optional placeholders
     * @param args the arguments to substitute into the message
     * @return a new Failure instance with the formatted message
     */
    public static Failure fail(Node<?> source, String message, Object... args) {
        return new Failure(source, format(message, args));
    }

    /**
     * Formats a collection of failures into a single error message.
     * <p>
     * This method creates a comprehensive error message that includes:
     * <ul>
     *   <li>A count of the total number of problems</li>
     *   <li>Each failure on its own line with its location (line:column)</li>
     *   <li>The specific error message for each failure</li>
     * </ul>
     * The resulting message is suitable for displaying to end users.
     * </p>
     *
     * @param failures the collection of failures to format
     * @return a formatted multi-line error message
     */
    public static String failMessage(Collection<Failure> failures) {
        return failures.stream().map(f -> {
            Location l = f.node().source().source();
            return "line " + l.getLineNumber() + ":" + l.getColumnNumber() + ": " + f.message();
        })
            .collect(
                Collectors.joining(
                    StringUtils.NEW_LINE,
                    format("Found {} problem{}\n", failures.size(), failures.size() > 1 ? "s" : StringUtils.EMPTY),
                    StringUtils.EMPTY
                )
            );
    }
}
