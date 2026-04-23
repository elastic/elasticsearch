/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for generating temporary and unique names for query expressions.
 */
public interface TemporaryNameGenerator {

    int TO_STRING_LIMIT = 16;

    static String temporaryName(Expression inner, Expression outer, int suffix) {
        String in = toString(inner);
        String out = toString(outer);
        return Attribute.rawTemporaryName(in, out, String.valueOf(suffix));
    }

    static String locallyUniqueTemporaryName(String inner) {
        return Attribute.rawTemporaryName(inner, "temp_name", (new NameId()).toString());
    }

    static String toString(Expression ex) {
        return ex instanceof AggregateFunction af ? af.functionName() : extractString(ex);
    }

    static String extractString(Expression ex) {
        return ex instanceof NamedExpression ne ? ne.name() : limitToString(ex.sourceText()).replace(' ', '_');
    }

    static String limitToString(String string) {
        return string.length() > TO_STRING_LIMIT ? string.substring(0, TO_STRING_LIMIT - 1) + ">" : string;
    }

    String next(String prefix);

    /**
     * Generates names with a monotonically increasing numeric suffix.
     */
    final class Monotonic implements TemporaryNameGenerator {
        private final Map<String, Integer> counters = new HashMap<>();

        @Override
        public String next(String prefix) {
            int id = counters.compute(prefix, (k, v) -> v == null ? 1 : v + 1);
            return prefix + "_$" + id;
        }
    }
}
