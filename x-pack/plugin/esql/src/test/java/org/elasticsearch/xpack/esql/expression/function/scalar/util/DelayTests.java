/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class DelayTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = List.of(new TestCaseSupplier(List.of(DataType.TIME_DURATION), () -> {
            long ms = randomLongBetween(0, 1);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(Duration.ofMillis(ms), DataType.TIME_DURATION, "ms").forceLiteral()),
                "DelayEvaluator[ms=" + ms + "]",
                DataType.BOOLEAN,
                equalTo(true)
            );
        }));
        return parameterSuppliersFromTypedData(suppliers);
    }

    public DelayTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Before
    public void requireSnapshot() {
        assumeTrue("DELAY is only available in snapshot builds", Build.current().isSnapshot());
    }

    @Override
    protected Matcher<Object> allNullsMatcher() {
        // The only argument is a literal, so no null is ever inserted into the input page
        return equalTo(true);
    }

    @Override
    protected int evaluateInManyThreadsCountMax() {
        return 100;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Delay(source, args.get(0));
    }

    public void testTypedNullFailsVerification() {
        Delay delay = new Delay(Source.EMPTY, new Literal(Source.EMPTY, null, DataType.TIME_DURATION));
        assertThat(postOptimizationFailures(delay), contains("first argument of [] cannot be null, received []"));
    }

    public void testNonConstantFailsVerification() {
        Delay delay = new Delay(Source.EMPTY, referenceAttribute("ms", DataType.TIME_DURATION));
        assertThat(postOptimizationFailures(delay), contains("first argument of [] must be a constant, received [ms]"));
    }

    private static List<String> postOptimizationFailures(Delay delay) {
        Failures failures = new Failures();
        delay.postOptimizationVerification(failures);
        return failures.failures().stream().map(Failure::message).toList();
    }

    @Override
    public void testFold() {
        // Delay must sleep for every row, so it cannot be folded
    }
}
