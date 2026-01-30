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

public class Failure {

    private final Node<?> node;
    private final String message;

    public Failure(Node<?> node, String message) {
        this.node = node;
        this.message = message;
    }

    public Node<?> node() {
        return node;
    }

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

    @Override
    public String toString() {
        return message;
    }

    public static Failure fail(Node<?> source, String message, Object... args) {
        return new Failure(source, format(message, args));
    }

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
