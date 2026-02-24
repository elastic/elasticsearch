/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.function.MathFunctionGenerator.binaryMathFunction;
import static org.elasticsearch.xpack.esql.generator.function.MathFunctionGenerator.clampFunction;
import static org.elasticsearch.xpack.esql.generator.function.MathFunctionGenerator.mathFunction;
import static org.elasticsearch.xpack.esql.generator.function.StringFunctionGenerator.concatFunction;
import static org.elasticsearch.xpack.esql.generator.function.StringFunctionGenerator.stringFunction;
import static org.elasticsearch.xpack.esql.generator.function.StringFunctionGenerator.stringToIntFunction;

/**
 * Helpers to generate expressions with a known compatible return type.
 */
public final class TypeSafeExpressionGenerator {

    private TypeSafeExpressionGenerator() {}

    /**
     * Generates a random expression that is guaranteed to return one of the given accepted types.
     * This should be used when the expression will be passed as an argument to a function with
     * specific type constraints (e.g. top(), greatest/least, etc.).
     * <p>
     * Prefers generating a function expression wrapping a compatible field, but falls back
     * to a plain field reference if no function can be generated.
     *
     * @param columns the available columns
     * @param acceptedTypes the set of types the calling function accepts (e.g. {"integer", "long", "double", "keyword", "date"})
     * @param allowUnmapped if true, may use unmapped field names
     * @return an expression string whose output type is in acceptedTypes, or null if none can be generated
     */
    public static String typeSafeExpression(List<Column> columns, Set<String> acceptedTypes, boolean allowUnmapped) {
        if (randomIntBetween(0, 10) < 5) {
            String funcExpr = typeSafeFunctionExpression(columns, acceptedTypes, allowUnmapped);
            if (funcExpr != null) {
                return funcExpr;
            }
        }
        return randomName(columns, acceptedTypes);
    }

    private static String typeSafeFunctionExpression(List<Column> columns, Set<String> acceptedTypes, boolean allowUnmapped) {
        boolean acceptsNumeric = acceptedTypes.contains("integer") || acceptedTypes.contains("long") || acceptedTypes.contains("double");
        boolean acceptsString = acceptedTypes.contains("keyword") || acceptedTypes.contains("text");
        boolean acceptsDate = acceptedTypes.contains("date") || acceptedTypes.contains("datetime");

        ArrayList<Supplier<String>> candidates = new ArrayList<>();

        if (acceptsNumeric) {
            candidates.add(() -> mathFunction(columns, allowUnmapped));
            candidates.add(() -> binaryMathFunction(columns, allowUnmapped));
            candidates.add(() -> stringToIntFunction(columns, allowUnmapped));
            candidates.add(() -> clampFunction(columns));
        }
        if (acceptsString) {
            candidates.add(() -> stringFunction(columns, allowUnmapped));
            candidates.add(() -> concatFunction(columns, allowUnmapped));
        }
        if (acceptsDate) {
            String dateField = randomName(columns, Set.of("date", "datetime"));
            if (dateField != null) {
                String interval = randomFrom("1 day", "1 hour", "1 week", "1 month", "1 year");
                candidates.add(() -> "date_trunc(" + interval + ", " + dateField + ")");
                candidates.add(() -> "now()");
            }
        }

        if (candidates.isEmpty()) {
            return null;
        }

        for (int attempt = 0; attempt < 3; attempt++) {
            String result = randomFrom(candidates).get();
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}

