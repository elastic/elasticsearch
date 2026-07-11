/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * A bound parameter for the JDBC pushed query. Pairs an already-folded Java value with its originating ESQL
 * {@link DataType} so {@link JdbcDialect#bindParam(java.sql.PreparedStatement, int, Object, DataType)} can choose
 * the right {@code setX} variant per vendor.
 * <p>
 * Literal values arrive here pre-folded by the optimizer or via {@code Expression.fold(FoldContext.small())} at
 * pushdown time -- {@link SqlParam} never holds an unfolded {@link org.elasticsearch.xpack.esql.core.expression.Expression}.
 *
 * @param value     Java value (may be {@code null} only for {@code IS NULL}-style predicates; binary comparisons
 *                  with a {@code null} RHS are rejected at translation time per the {@code In} no-null rule).
 * @param esqlType  the ESQL data type at the predicate's call site; used by {@code bindParam} to disambiguate
 *                  e.g. {@code DATETIME (Long millis)} vs {@code LONG}.
 */
public record SqlParam(Object value, DataType esqlType) {
    public SqlParam {
        if (esqlType == null) {
            throw new IllegalArgumentException("esqlType must not be null");
        }
    }
}
