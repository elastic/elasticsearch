/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;

/**
 * Shared helpers used by the random function generators.
 */
final class FunctionGeneratorUtils {

    /**
     * Probability (0-100) of using an unmapped field name instead of a real field.
     * This tests how functions handle NULL data type from unmapped fields.
     */
    private static final int UNMAPPED_FIELD_PROBABILITY = 10;

    /**
     * Types that are NOT accepted by most scalar functions. These are special metric/internal types
     * that should be excluded when selecting fields for general-purpose function arguments.
     */
    private static final Set<String> SCALAR_UNSUPPORTED_TYPES = Set.of(
        "counter_long",
        "counter_double",
        "counter_integer",
        "aggregate_metric_double",
        "dense_vector",
        "flattened",
        "tdigest",
        "histogram",
        "exponential_histogram",
        "date_range"
    );

    private FunctionGeneratorUtils() {}

    static boolean shouldAddUnmappedField() {
        return shouldAddUnmappedFieldWithProbabilityIncrease(1);
    }

    static boolean shouldAddUnmappedFieldWithProbabilityIncrease(int probabilityIncrease) {
        assert probabilityIncrease > 0 && probabilityIncrease < 10 : "Probability increase should be in interval [1, 9]";
        return randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY * probabilityIncrease;
    }

    /**
     * Returns an unmapped field name with some probability, otherwise returns null.
     */
    static String maybeUnmappedField(boolean allowUnmapped) {
        if (allowUnmapped == false) {
            return null;
        }
        return shouldAddUnmappedField() ? randomUnmappedFieldName() : null;
    }

    /**
     * Returns a field name, with some probability returning an unmapped field name instead.
     */
    static String fieldOrUnmapped(String realField, boolean allowUnmapped) {
        if (realField == null) {
            return null;
        }
        String unmapped = maybeUnmappedField(allowUnmapped);
        return unmapped != null ? unmapped : realField;
    }

    /**
     * Returns a field name suitable for use as a scalar function argument.
     * Excludes types that are rejected by most scalar functions (counter types, aggregate_metric_double, etc.).
     */
    static String randomScalarField(List<Column> columns) {
        List<Column> suitable = columns.stream().filter(c -> SCALAR_UNSUPPORTED_TYPES.contains(c.type()) == false).toList();
        if (suitable.isEmpty()) {
            return null;
        }
        return randomName(suitable);
    }
}
