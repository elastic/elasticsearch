/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate;

import java.util.Locale;
import java.util.function.BiFunction;

public interface PredicateBiFunction<T, U, R> extends BiFunction<T, U, R> {

    String name();

    String symbol();

    @Override
    default R apply(T t, U u) {
        if (t == null || u == null) {
            return null;
        }

        return doApply(t, u);
    }

    R doApply(T t, U u);

    default String scriptMethodName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
