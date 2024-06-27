/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Base class for aggregation tests.
 */
public abstract class AbstractAggregationTestCase extends AbstractFunctionTestCase {
    /**
     * Converts a list of aggregation test cases into a list of parameter suppliers.
     * Also, adds a default set of extra test cases.
     * <p>
     *     Use if possible, as this method may get updated with new checks in the future.
     * </p>
     */
    protected static Iterable<Object[]> parameterSuppliersFromTypedDataWithDefaultChecks(List<TestCaseSupplier> suppliers) {
        // TODO: Add case with no input expecting null
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    public void testAggregate() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);

        resolveExpression(expression, this::aggregateSingleMode, this::evaluate);
    }

    public void testFold() {
        Expression expression = buildLiteralExpression(testCase);

        resolveExpression(expression, aggregatorFunctionSupplier -> {
            // An aggregation cannot be folded
        }, evaluableExpression -> {
            assertTrue(evaluableExpression.foldable());
            if (testCase.foldingExceptionClass() == null) {
                Object result = evaluableExpression.fold();
                // Decode unsigned longs into BigIntegers
                if (testCase.expectedType() == DataType.UNSIGNED_LONG && result != null) {
                    result = NumericUtils.unsignedLongAsBigInteger((Long) result);
                }
                assertThat(result, testCase.getMatcher());
                if (testCase.getExpectedWarnings() != null) {
                    assertWarnings(testCase.getExpectedWarnings());
                }
            } else {
                Throwable t = expectThrows(testCase.foldingExceptionClass(), evaluableExpression::fold);
                assertThat(t.getMessage(), equalTo(testCase.foldingExceptionMessage()));
            }
        });
    }

    private void aggregateSingleMode(AggregatorFunctionSupplier aggregatorFunctionSupplier) {
        Object result;
        try (var aggregator = new Aggregator(aggregatorFunctionSupplier.aggregator(driverContext()), AggregatorMode.SINGLE)) {
            Page inputPage = rows(testCase.getMultiRowDataValues());
            try {
                aggregator.processPage(inputPage);
            } finally {
                inputPage.releaseBlocks();
            }

            // ElementType from DataType
            result = extractResultFromAggregator(aggregator, PlannerUtils.toElementType(testCase.expectedType()));
        }

        assertThat(result, not(equalTo(Double.NaN)));
        assert testCase.getMatcher().matches(Double.POSITIVE_INFINITY) == false;
        assertThat(result, not(equalTo(Double.POSITIVE_INFINITY)));
        assert testCase.getMatcher().matches(Double.NEGATIVE_INFINITY) == false;
        assertThat(result, not(equalTo(Double.NEGATIVE_INFINITY)));
        assertThat(result, testCase.getMatcher());
        if (testCase.getExpectedWarnings() != null) {
            assertWarnings(testCase.getExpectedWarnings());
        }
    }

    private void evaluate(Expression evaluableExpression) {
        Object result;
        try (var evaluator = evaluator(evaluableExpression).get(driverContext())) {
            try (Block block = evaluator.eval(row(testCase.getDataValues()))) {
                result = toJavaObjectUnsignedLongAware(block, 0);
            }
        }

        assertThat(result, not(equalTo(Double.NaN)));
        assert testCase.getMatcher().matches(Double.POSITIVE_INFINITY) == false;
        assertThat(result, not(equalTo(Double.POSITIVE_INFINITY)));
        assert testCase.getMatcher().matches(Double.NEGATIVE_INFINITY) == false;
        assertThat(result, not(equalTo(Double.NEGATIVE_INFINITY)));
        assertThat(result, testCase.getMatcher());
        if (testCase.getExpectedWarnings() != null) {
            assertWarnings(testCase.getExpectedWarnings());
        }
    }

    private void resolveExpression(
        Expression expression,
        Consumer<AggregatorFunctionSupplier> onAggregator,
        Consumer<Expression> onEvaluableExpression
    ) {
        logger.info(
            "Test Values: " + testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","))
        );

        if (checkExpectedErrors(expression)) {
            return;
        }

        expression = resolveSurrogates(expression);

        // No expected errors, but we want to check that the surrogate is ok
        checkExpectedErrors(expression);

        expression = new FoldNull().rule(expression);
        assertThat(expression.dataType(), equalTo(testCase.expectedType()));

        if (expression instanceof AggregateFunction == false) {
            onEvaluableExpression.accept(expression);
            return;
        }

        assertThat(expression, instanceOf(ToAggregator.class));
        logger.info("Result type: " + expression.dataType());

        var inputChannels = inputChannels();
        onAggregator.accept(((ToAggregator) expression).supplier(inputChannels));
    }

    private Object extractResultFromAggregator(Aggregator aggregator, ElementType expectedElementType) {
        var blocksArraySize = randomIntBetween(1, 10);
        var resultBlockIndex = randomIntBetween(0, blocksArraySize - 1);
        var blocks = new Block[blocksArraySize];
        try {
            aggregator.evaluate(blocks, resultBlockIndex, driverContext());

            var block = blocks[resultBlockIndex];

            assertThat(block.elementType(), equalTo(expectedElementType));

            return toJavaObject(blocks[resultBlockIndex], 0);
        } finally {
            Releasables.close(blocks);
        }
    }

    private List<Integer> inputChannels() {
        // TODO: Randomize channels
        // TODO: If surrogated, channels may change
        return IntStream.range(0, testCase.getMultiRowDataValues().size()).boxed().toList();
    }

    /**
     * Resolves surrogates of aggregations until a non-surrogate expression is found.
     * <p>
     *     No-op if expecting errors, as surrogates depend on correct types
     * </p>
     */
    private Expression resolveSurrogates(Expression expression) {
        if (testCase.getExpectedTypeError() != null) {
            return expression;
        }

        for (int i = 0;; i++) {
            assertThat("Potential infinite loop detected in surrogates", i, lessThan(10));

            if (expression instanceof SurrogateExpression == false) {
                break;
            }

            var surrogate = ((SurrogateExpression) expression).surrogate();

            if (surrogate == null) {
                break;
            }

            expression = surrogate;
        }

        return expression;
    }
}
