/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfigurationBuilder;
import static org.hamcrest.Matchers.equalTo;

public class TStepErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    private static final List<DataType> BOUND_TYPES = List.of(DataType.DATETIME, DataType.DATE_NANOS, DataType.KEYWORD, DataType.TEXT);
    private static final List<DataType> TIMESTAMP_TYPES = List.of(DataType.DATETIME, DataType.DATE_NANOS);
    private static final List<DataType> COUNT_STEP_TYPES = List.of(DataType.INTEGER, DataType.LONG);

    @Override
    protected List<TestCaseSupplier> cases() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        addUnboundedSignatures(suppliers, DataType.TIME_DURATION);
        COUNT_STEP_TYPES.forEach(stepType -> addUnboundedSignatures(suppliers, stepType));
        addBoundedSignatures(suppliers, DataType.TIME_DURATION);
        COUNT_STEP_TYPES.forEach(stepType -> addBoundedSignatures(suppliers, stepType));

        return suppliers;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        var configuration = randomConfigurationBuilder().query(source.text()).build();
        return switch (args.size()) {
            case 2 -> {
                var anchor = configuration.now();
                yield new TStep(source, args.get(0), args.get(1), configuration).withTimestampBounds(
                    Literal.dateTime(source, anchor),
                    Literal.dateTime(source, anchor)
                );
            }
            case 4 -> new TStep(source, args.get(0), args.get(1), args.get(2), args.get(3), configuration);
            default -> throw new IllegalArgumentException("unexpected TSTEP arity " + args.size());
        };
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        return super.testCandidates(cases, valid).filter(signature -> signature.contains(DataType.NULL) == false);
    }

    private static void addUnboundedSignatures(List<TestCaseSupplier> suppliers, DataType stepType) {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            suppliers.add(signature(stepType, timestampType));
        }
    }

    private static void addBoundedSignatures(List<TestCaseSupplier> suppliers, DataType stepType) {
        for (DataType fromType : BOUND_TYPES) {
            for (DataType toType : BOUND_TYPES) {
                for (DataType timestampType : TIMESTAMP_TYPES) {
                    suppliers.add(signature(stepType, fromType, toType, timestampType));
                }
            }
        }
    }

    private static TestCaseSupplier signature(DataType... types) {
        return new TestCaseSupplier(List.of(types), () -> null);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return switch (signature.size()) {
            case 2 -> expectedTwoArgTypeError(signature);
            case 4 -> expectedFourArgTypeError(signature);
            default -> throw new IllegalStateException("Can't generate error message for signature " + signature);
        };
    }

    private static Matcher<String> expectedTwoArgTypeError(List<DataType> signature) {
        if (isValidStepType(signature.get(0)) == false) {
            return typeError(signature, 0, "first", "time_duration, integer or long");
        }
        if (isValidTimestampType(signature.get(1)) == false) {
            return typeError(signature, 1, "implicit", "date_nanos or datetime");
        }
        throw new IllegalStateException("Expected invalid two-argument TSTEP signature " + signature);
    }

    private static Matcher<String> expectedFourArgTypeError(List<DataType> signature) {
        if (isValidStepType(signature.get(0)) == false) {
            return typeError(signature, 0, "first", "time_duration, integer or long");
        }
        if (isValidTimestampType(signature.get(3)) == false) {
            return typeError(signature, 3, "implicit", "date_nanos or datetime");
        }
        if (isValidBoundType(signature.get(1)) == false) {
            return typeError(signature, 1, "second", "date_nanos or datetime or string");
        }
        if (isValidBoundType(signature.get(2)) == false) {
            return typeError(signature, 2, "third", "date_nanos or datetime or string");
        }
        throw new IllegalStateException("Expected invalid four-argument TSTEP signature " + signature);
    }

    private static Matcher<String> typeError(List<DataType> signature, int badArgPosition, String ordinal, String expectedType) {
        return equalTo(
            ordinal
                + " argument of ["
                + sourceForSignature(signature)
                + "] must be ["
                + expectedType
                + "], found value [] type ["
                + signature.get(badArgPosition).typeName()
                + "]"
        );
    }

    private static boolean isValidStepType(DataType type) {
        return type == DataType.TIME_DURATION || COUNT_STEP_TYPES.contains(type);
    }

    private static boolean isValidBoundType(DataType type) {
        return DataType.isString(type) || DataType.isMillisOrNanos(type);
    }

    private static boolean isValidTimestampType(DataType type) {
        return DataType.isMillisOrNanos(type);
    }
}
