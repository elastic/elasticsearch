/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

@FunctionalInterface
public interface ColumnProcessor {

    Object apply(Object r);

    default ColumnProcessor andThen(ColumnProcessor after) {
        return after != null ? r -> after.apply(apply(r)) : this;
    }
}
