/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

/**
 * Thrown by internal generator calls (e.g. subquery generation) when an inner query fails with a
 * known/allowed error. Callers that intercept generator exceptions should treat this as a signal
 * that the failure is expected and safe to ignore, rather than a test-breaking condition.
 */
public final class AllowedGeneratorFailureException extends RuntimeException {
    private final String query;

    public AllowedGeneratorFailureException(String query, Exception cause) {
        super(cause.getMessage(), cause);
        this.query = query;
    }

    /**
     * The generated query that failed with an allowed error.
     */
    public String query() {
        return query;
    }
}
