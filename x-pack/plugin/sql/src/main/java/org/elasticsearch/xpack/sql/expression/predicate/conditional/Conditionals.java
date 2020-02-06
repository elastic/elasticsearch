/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Comparisons;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

final class Conditionals {

    private Conditionals() {}

    static Object coalesce(Collection<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        for (Object object : values) {
            if (object != null) {
                return object;
            }
        }

        return null;
    }

    static Object coalesceInput(List<Processor> processors, Object input) {
        for (Processor proc : processors) {
            Object result = proc.process(input);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    static Object greatest(Collection<Object> values) {
        return extremum(values, Comparisons::gt);
    }

    static Object greatestInput(Collection<Processor> processors, Object input) {
        List<Object> values = new ArrayList<>(processors.size());
        for (Processor processor : processors) {
            values.add(processor.process(input));
        }
        return greatest(values);
    }

    static Object least(Collection<Object> values) {
        return extremum(values, Comparisons::lt);
    }

    static Object leastInput(List<Processor> processors, Object input) {
        List<Object> values = new ArrayList<>(processors.size());
        for (Processor processor : processors) {
            values.add(processor.process(input));
        }
        return least(values);
    }

    private static Object extremum(Collection<Object> values, BiFunction<Object, Object, Boolean> comparison) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        Object result = null;
        boolean isFirst = true;
        for (Object value : values) {
            if (isFirst || (result == null) || (comparison.apply(value, result) == Boolean.TRUE)) {
                result = value;
            }
            isFirst = false;
        }
        return result;
    }
}
