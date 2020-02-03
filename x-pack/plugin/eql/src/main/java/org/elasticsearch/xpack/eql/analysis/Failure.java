/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.tree.Node;

import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

class Failure {

    private final Node<?> node;
    private final String message;

    Failure(Node<?> node, String message) {
        this.node = node;
        this.message = message;
    }

    Node<?> node() {
        return node;
    }

    String message() {
        return message;
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, node);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Failure other = (Failure) obj;
        return Objects.equals(message, other.message) && Objects.equals(node, other.node);
    }

    @Override
    public String toString() {
        return message;
    }

    static Failure fail(Node<?> source, String message, Object... args) {
        return new Failure(source, format(message, args));
    }
}
