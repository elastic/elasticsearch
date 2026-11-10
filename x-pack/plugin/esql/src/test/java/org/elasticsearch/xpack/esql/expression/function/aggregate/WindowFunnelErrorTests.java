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
        WindowFunnel funnel = new WindowFunnel(Source.synthetic("f"), maxspan(Duration.ofHours(1)), timestamp(), List.of(), TEST_CFG);
        assertThat(funnel.typeResolved().message(), equalTo("[f] requires at least one condition"));
    }

    public void testTooManyConditions() {
        List<Expression> conditions = Collections.nCopies(WindowFunnel.MAX_CONDITIONS + 1, randomLiteral(DataType.BOOLEAN));
        WindowFunnel funnel = new WindowFunnel(Source.synthetic("f"), maxspan(Duration.ofHours(1)), timestamp(), conditions, TEST_CFG);
        assertThat(funnel.typeResolved().message(), equalTo("[f] supports at most [32] conditions, found [33]"));
    }

    public void testMaxConditions() {
        List<Expression> conditions = Collections.nCopies(WindowFunnel.MAX_CONDITIONS, randomLiteral(DataType.BOOLEAN));
        WindowFunnel funnel = new WindowFunnel(Source.synthetic("f"), maxspan(Duration.ofHours(1)), timestamp(), conditions, TEST_CFG);
        assertTrue(funnel.typeResolved().resolved());
    }

    public void testNonConstantMaxspan() {
        WindowFunnel funnel = new WindowFunnel(
            Source.synthetic("f"),
            field("span", DataType.TIME_DURATION),
            timestamp(),
            List.of(randomLiteral(DataType.BOOLEAN)),
            TEST_CFG
        );
        assertThat(funnel.typeResolved().message(), equalTo("first argument of [f] must be a constant, received [span]"));
    }

    public void testNegativeMaxspan() {
        Expression maxspan = Literal.timeDuration(Source.synthetic("-1 hour"), Duration.ofHours(-1));
        WindowFunnel funnel = new WindowFunnel(
            Source.synthetic("f"),
            maxspan,
            timestamp(),
            List.of(randomLiteral(DataType.BOOLEAN)),
            TEST_CFG
        );
        assertThat(funnel.typeResolved().message(), equalTo("first argument of [f] must be a non-negative time_duration, found [-1 hour]"));
    }

    public void testNullValuedMaxspan() {
        Expression maxspan = new Literal(Source.synthetic("span"), null, DataType.TIME_DURATION);
        WindowFunnel funnel = new WindowFunnel(
            Source.synthetic("f"),
            maxspan,
            timestamp(),
            List.of(randomLiteral(DataType.BOOLEAN)),
            TEST_CFG
        );
        assertThat(funnel.typeResolved().message(), equalTo("first argument of [f] cannot be null, received [span]"));
    }

    public void testZeroMaxspan() {
        WindowFunnel funnel = new WindowFunnel(
            Source.synthetic("f"),
            maxspan(Duration.ZERO),
            timestamp(),
            List.of(randomLiteral(DataType.BOOLEAN)),
            TEST_CFG
        );
        assertTrue(funnel.typeResolved().resolved());
    }

    public void testLevelRequiresLiteralMaxspan() {
        Expression maxspan = field("span", DataType.TIME_DURATION);
        WindowFunnelLevel level = new WindowFunnelLevel(
            Source.synthetic("f"),
            timestamp(),
            field("mask", DataType.LONG),
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            maxspan,
            Literal.integer(Source.EMPTY, 1)
        );
        Failures failures = new Failures();
        level.postOptimizationVerification(failures);
        assertThat(failures.failures(), hasSize(1));
        assertThat(
            failures.failures().iterator().next().message(),
            equalTo("maxspan of [f] must be a constant time_duration, found [" + maxspan + "]")
        );
    }

    public void testLevelRejectsNullMaxspan() {
        assertThat(
            levelFailure(new Literal(Source.synthetic("span"), null, DataType.TIME_DURATION)),
            equalTo("first argument of [f] cannot be null, received [span]")
        );
    }

    public void testLevelRejectsNegativeMaxspan() {
        assertThat(
            levelFailure(Literal.timeDuration(Source.synthetic("-1 hour"), Duration.ofHours(-1))),
            equalTo("first argument of [f] must be a non-negative time_duration, found [-1 hour]")
        );
    }

    public void testLevelAcceptsZeroMaxspan() {
        Failures failures = new Failures();
        level(maxspan(Duration.ZERO)).postOptimizationVerification(failures);
        assertFalse(failures.hasFailures());
    }

    private static String levelFailure(Expression maxspan) {
        Failures failures = new Failures();
        level(maxspan).postOptimizationVerification(failures);
        assertThat(failures.failures(), hasSize(1));
        return failures.failures().iterator().next().message();
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
