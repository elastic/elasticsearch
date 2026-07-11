/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

/**
 * Opaque token stored on {@link org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec#pushedFilter()} for
 * the JDBC connector. Built by {@link JdbcFilterPushdownSupport#pushFilters} during local physical optimization,
 * read by {@link JdbcConnector} when it builds the SQL.
 * <p>
 * External sources execute on the coordinator only -- {@link JdbcPushedQuery} is therefore never serialized
 * across the wire. The record holds an already-translated {@link SqlPredicate} tree; rendering to parameterized
 * SQL is deferred to {@link JdbcQueryBuilder} via {@link SqlRenderer} so the dialect-aware quoting is applied
 * exactly once at execute time.
 */
public record JdbcPushedQuery(SqlPredicate filter) {
    public JdbcPushedQuery {
        if (filter == null) {
            throw new IllegalArgumentException("filter must not be null");
        }
    }
}
