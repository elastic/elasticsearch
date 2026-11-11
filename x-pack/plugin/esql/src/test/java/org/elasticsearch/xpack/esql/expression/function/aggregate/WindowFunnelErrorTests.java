/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.validFunctionParameters;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class WindowFunnelErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(WindowFunnelTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new WindowFunnel(source, args.get(0), args.get(1), args.subList(2, args.size()), TEST_CFG);
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // Checking every permutation is too expensive, so each valid signature has one argument replaced
        return valid.stream()
            .flatMap(
                signature -> IntStream.range(0, signature.size())
                    .boxed()
                    .flatMap(position -> validFunctionParameters().map(type -> replace(signature, position, type)))
            )
            .distinct()
            .filter(signature -> valid.contains(signature) == false);
    }

    private static List<DataType> replace(List<DataType> signature, int position, DataType type) {
        List<DataType> replaced = new ArrayList<>(signature);
        replaced.set(position, type);
        return replaced;
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.get(0) == DataType.NULL) {
            return equalTo("first argument of [" + sourceForSignature(signature) + "] cannot be null, received []");
        }
        for (int i = 0; i < signature.size(); i++) {
            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                if (TypeResolutions.ParamOrdinal.fromIndex(i) == TypeResolutions.ParamOrdinal.DEFAULT) {
                    return equalTo(
                        "argument of ["
                            + sourceForSignature(signature)
                            + "] must be [boolean], found value [] type ["
                            + signature.get(i).typeName()
                            + "]"
                    );
                }
                return typeErrorMessage(signature, i, i == 0 ? "time_duration" : i == 1 ? "date or date_nanos" : "boolean");
            }
        }
        throw new IllegalStateException("can't make error message for " + signature);
    }

    public void testNoConditions() {
        assertThat(
            funnel(maxspan(Duration.ofHours(1)), List.of()).typeResolved().message(),
            equalTo("[f] requires at least one condition")
        );
    }

    public void testTooManyConditions() {
        List<Expression> conditions = Collections.nCopies(WindowFunnel.MAX_CONDITIONS + 1, randomLiteral(DataType.BOOLEAN));
        assertThat(
            funnel(maxspan(Duration.ofHours(1)), conditions).typeResolved().message(),
            equalTo("[f] supports at most [32] conditions, found [33]")
        );
    }

    public void testMaxConditions() {
        List<Expression> conditions = Collections.nCopies(WindowFunnel.MAX_CONDITIONS, randomLiteral(DataType.BOOLEAN));
        assertTrue(funnel(maxspan(Duration.ofHours(1)), conditions).typeResolved().resolved());
    }

    public void testNonConstantMaxspan() {
        assertThat(
            funnel(field("span", DataType.TIME_DURATION)).typeResolved().message(),
            equalTo("first argument of [f] must be a constant, received [span]")
        );
    }

    public void testNegativeMaxspan() {
        Expression maxspan = Literal.timeDuration(Source.synthetic("-1 hour"), Duration.ofHours(-1));
        assertThat(
            funnel(maxspan).typeResolved().message(),
            equalTo("first argument of [f] must be a non-negative time_duration, found [-1 hour]")
        );
    }

    public void testNullValuedMaxspan() {
        Expression maxspan = new Literal(Source.synthetic("span"), null, DataType.TIME_DURATION);
        assertThat(funnel(maxspan).typeResolved().message(), equalTo("first argument of [f] cannot be null, received [span]"));
    }

    public void testZeroMaxspan() {
        assertTrue(funnel(maxspan(Duration.ZERO)).typeResolved().resolved());
    }

    public void testDatePeriodMaxspan() {
        Expression maxspan = new Literal(Source.synthetic("1 day"), Period.ofDays(1), DataType.DATE_PERIOD);
        assertThat(
            funnel(maxspan).typeResolved().message(),
            equalTo(
                "first argument of [f] must be [time_duration], found value [1 day] type [date_period]"
                    + "; a date_period is calendar based, use a time_duration such as [24 hours] instead"
            )
        );
    }

    public void testLevelRequiresLiteralMaxspan() {
        Expression maxspan = field("span", DataType.TIME_DURATION);
        Failures failures = new Failures();
        level(maxspan).postOptimizationVerification(failures);
        assertThat(failures.failures(), hasSize(1));
        assertThat(
            failures.failures().iterator().next().message(),
            equalTo("maxspan of [f] must be a constant time_duration, found [" + maxspan + "]")
        );
    }

    public void testLevelAcceptsZeroMaxspan() {
        Failures failures = new Failures();
        level(maxspan(Duration.ZERO)).postOptimizationVerification(failures);
        assertFalse(failures.hasFailures());
    }

    private static WindowFunnel funnel(Expression maxspan) {
        return funnel(maxspan, List.of(randomLiteral(DataType.BOOLEAN)));
    }

    private static WindowFunnel funnel(Expression maxspan, List<Expression> conditions) {
        return new WindowFunnel(Source.synthetic("f"), maxspan, timestamp(), conditions, TEST_CFG);
    }

    private static WindowFunnelLevel level(Expression maxspan) {
        return new WindowFunnelLevel(
            Source.synthetic("f"),
            timestamp(),
            field("mask", DataType.LONG),
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            maxspan,
            Literal.integer(Source.EMPTY, 1)
        );
    }

    private static Expression maxspan(Duration duration) {
        return Literal.timeDuration(Source.EMPTY, duration);
    }

    private static Expression timestamp() {
        return field("@timestamp", randomFrom(DataType.DATETIME, DataType.DATE_NANOS));
    }
}
