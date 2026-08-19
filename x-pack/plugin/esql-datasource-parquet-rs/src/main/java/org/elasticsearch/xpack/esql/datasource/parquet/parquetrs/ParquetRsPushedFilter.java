/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.List;
import java.util.Objects;

/**
 * Plain Java description of the filters that {@link ParquetRsFilterPushdownSupport} accepted
 * for pushdown. The native {@code FilterExpr} tree is built lazily inside
 * {@link ParquetRsFormatReader#read} and freed in the same call, so this record never
 * carries a JNI handle.
 * <p>
 * Two consequences of carrying Expressions instead of a native handle:
 * <ul>
 *   <li>No native memory escapes a single {@code read()} call, so there is no per-query or
 *       per-file leak even if the {@link ParquetRsFormatReader} is never closed.</li>
 *   <li>Equality is structural and {@link Object#hashCode()} is meaningful, which matches
 *       {@code ExternalSourceExec.equals} expectations.</li>
 * </ul>
 */
record ParquetRsPushedFilter(List<Expression> pushedExpressions) {

    ParquetRsPushedFilter {
        Objects.requireNonNull(pushedExpressions, "pushedExpressions");
        pushedExpressions = List.copyOf(pushedExpressions);
    }

    @Override
    public String toString() {
        if (pushedExpressions.isEmpty()) {
            return "none";
        }
        return pushedExpressions.toString();
    }
}
