/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramPercentile;

import java.util.List;
import java.util.function.Supplier;

public class PercentileOverTimeTests extends AbstractAggregationTestCase {
    public PercentileOverTimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
        if (testCase.getData().getFirst().type().isHistogram()) {
            testCase = testCase.withInjectNullTemporality();
        }
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return PercentileTests.parameters();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new PercentileOverTime(source, args.get(0), args.get(1), Literal.NULL);
    }

    @Override
    public void testAggregate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    @Override
    public void testAggregateToString() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    @Override
    public void testAggregateIntermediate() {
        assumeTrue("time-series aggregation doesn't support ungrouped", false);
    }

    @Override
    public void testGroupingAggregate() {
        if (testCase.getData().getFirst().type() == DataType.EXPONENTIAL_HISTOGRAM) {
            // Can't execute the aggregator because additional inputs (e.g. timestamp) are missing; verify the surrogate structure instead.
            assertExpHistogramSurrogate(buildFieldExpression(testCase));
            return;
        }
        super.testGroupingAggregate();
    }

    private void assertExpHistogramSurrogate(Expression expression) {
        assumeTrue("expression should have no type errors", expression.typeResolved().resolved());
        Expression surrogate = ((SurrogateExpression) expression).surrogate();
        assertNotNull(surrogate);
        assertTrue("expected HistogramPercentile, got: " + surrogate.getClass().getSimpleName(), surrogate instanceof HistogramPercentile);
        HistogramPercentile hp = (HistogramPercentile) surrogate;
        assertTrue("expected HistogramMergeOverTime", hp.children().getFirst() instanceof HistogramMergeOverTime);
    }

    @Override
    public void testFold() {
        assumeFalse(
            "exponential histogram fold tested via HistogramMergeOverTimeTests",
            testCase.getData().getFirst().type() == DataType.EXPONENTIAL_HISTOGRAM
        );
        super.testFold();
    }

}
