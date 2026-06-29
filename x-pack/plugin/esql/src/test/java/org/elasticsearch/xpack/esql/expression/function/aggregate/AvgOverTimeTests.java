/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.HistogramBlock;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;

public class AvgOverTimeTests extends AbstractAggregationTestCase {

    public AvgOverTimeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
        return AvgTests.parameters();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new AvgOverTime(source, args.get(0), AggregateFunction.NO_WINDOW, Literal.NULL);
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

    @Override
    public void testFold() {
        assumeFalse(
            "exponential histogram fold tested via HistogramMergeOverTimeTests",
            testCase.getData().getFirst().type() == DataType.EXPONENTIAL_HISTOGRAM
        );
        super.testFold();
    }

    private void assertExpHistogramSurrogate(Expression expression) {
        assumeTrue("expression should have no type errors", expression.typeResolved().resolved());
        Expression surrogate = ((SurrogateExpression) expression).surrogate();
        assertNotNull(surrogate);
        assertTrue("expected Div, got: " + surrogate.getClass().getSimpleName(), surrogate instanceof Div);
        Div div = (Div) surrogate;
        assertTrue("expected ExtractHistogramComponent for sum", div.left() instanceof ExtractHistogramComponent);
        assertTrue("expected ExtractHistogramComponent for count", div.right() instanceof ExtractHistogramComponent);
        ExtractHistogramComponent sum = (ExtractHistogramComponent) div.left();
        ExtractHistogramComponent count = (ExtractHistogramComponent) div.right();
        assertTrue("expected HistogramMergeOverTime for sum field", sum.field() instanceof HistogramMergeOverTime);
        assertTrue("expected HistogramMergeOverTime for count field", count.field() instanceof HistogramMergeOverTime);
        assertEquals(HistogramBlock.Component.SUM.ordinal(), ((Literal) sum.componentOrdinal()).value());
        assertEquals(HistogramBlock.Component.COUNT.ordinal(), ((Literal) count.componentOrdinal()).value());
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        ArrayList<DocsV3Support.Param> copies = new ArrayList<>(params);
        var preview = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", false);
        DocsV3Support.Param window = new DocsV3Support.Param(DataType.TIME_DURATION, List.of(preview));
        copies.add(window);
        return copies;
    }

    /**
     * Filters out implicitly injected parameters to ensure CONSTANT hint validation
     * only checks declared @Param arguments.
     */
    public static List<TestCaseSupplier.TypedData> providedParameters(List<TestCaseSupplier.TypedData> params) {
        return params;
    }
}
