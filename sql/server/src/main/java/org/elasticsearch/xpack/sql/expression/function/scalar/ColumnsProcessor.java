/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import java.util.Objects;

@FunctionalInterface
public interface ColumnsProcessor {

    Object apply(Object t);

    default ColumnsProcessor andThen(ColumnsProcessor after) {
        Objects.requireNonNull(after);
        return t -> after.apply(apply(t));
    }
}
