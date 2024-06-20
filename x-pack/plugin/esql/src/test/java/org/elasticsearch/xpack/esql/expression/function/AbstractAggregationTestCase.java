/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.optimizer.rules.SubstituteSurrogates;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Base class for aggregation tests.
 */
public abstract class AbstractAggregationTestCase extends AbstractFunctionTestCase {

    public void testEvaluate() {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", EsqlDataTypes.isRepresentable(testCase.expectedType()));
        logger.info(
            "Test Values: " + testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","))
        );
        boolean readFloating = randomBoolean();
        Expression expression = readFloating ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assertThat(expression, instanceOf(AggregateFunction.class));
        Expression.TypeResolution resolution = expression.typeResolved();
        if (resolution.unresolved()) {
            throw new AssertionError("expected resolved " + resolution.message());
        }
        expression = new FoldNull().rule(expression);
        assertThat(expression.dataType(), equalTo(testCase.expectedType()));
        logger.info("Result type: " + expression.dataType());

        expression = resolveSurrogates(expression);

        assertThat(expression, instanceOf(ToAggregator.class));

        var inputChannels = generateInputChannels();
        var aggregatorFunctionSupplier = ((ToAggregator) expression).supplier(inputChannels);


        Object result;
        try (var aggregator = new Aggregator(aggregatorFunctionSupplier.aggregator(driverContext()), AggregatorMode.SINGLE)) {
            aggregator.processPage(rows(testCase.getMultiRowDataValues()));

            var blocks = new Block[1];
            aggregator.evaluate(blocks, 0, driverContext());
            try (var block = blocks[0]) {
                result = toJavaObject(block, 0);
            }
        }

        // TODO: Tests for grouping aggregators
        // TODO: Tests for different AggregatorModes

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

    private Expression resolveSurrogates(Expression expression) {
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

        assumeTrue("Surrogate scalar functions not supported yet", expression instanceof AggregateFunction);

        return expression;
    }

    private List<Integer> generateInputChannels() {
        // TODO: Make the input channels based on TestData types
        return List.of(0);
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern..
     * <p>
     * Note that this'll sometimes be a {@link Vector} of values if the
     * input pattern contained only a single value.
     * </p>
     */
    @Ignore("TODO: UNIGNORE AND FIX")
    public void testEvaluateBlockWithoutNulls() {
        assumeTrue("no warning is expected", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), driverContext(), false);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
            assertFalse("Test data is too large to fit in the memory", true);
        }
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between.
     */
    @Ignore("TODO: UNIGNORE AND FIX")
    public void testEvaluateBlockWithNulls() {
        assumeTrue("no warning is expected", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), driverContext(), true);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
            assertFalse("Test data is too large to fit in the memory", true);
        }
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern,
     * using the {@link CrankyCircuitBreakerService} which fails randomly.
     * <p>
     * Note that this'll sometimes be a {@link Vector} of values if the
     * input pattern contained only a single value.
     * </p>
     */
    @Ignore("TODO: UNIGNORE AND FIX")
    public void testCrankyEvaluateBlockWithoutNulls() {
        assumeTrue("sometimes the cranky breaker silences warnings, just skip these cases", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), crankyContext(), false);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between, using the {@link CrankyCircuitBreakerService} which fails randomly.
     */
    @Ignore("TODO: UNIGNORE AND FIX")
    public void testCrankyEvaluateBlockWithNulls() {
        assumeTrue("sometimes the cranky breaker silences warnings, just skip these cases", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), crankyContext(), true);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testEvaluateBlock(BlockFactory inputBlockFactory, DriverContext context, boolean insertNulls) {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", EsqlDataTypes.isRepresentable(testCase.expectedType()));
        int positions = between(1, 1024);
        List<TestCaseSupplier.TypedData> data = testCase.getData();
        Page onePositionPage = row(testCase.getDataValues());
        Block[] manyPositionsBlocks = new Block[Math.toIntExact(data.stream().filter(d -> d.isForceLiteral() == false).count())];
        Set<Integer> nullPositions = insertNulls
            ? IntStream.range(0, positions).filter(i -> randomBoolean()).mapToObj(Integer::valueOf).collect(Collectors.toSet())
            : Set.of();
        if (nullPositions.size() == positions) {
            nullPositions = Set.of();
        }
        try {
            int b = 0;
            for (TestCaseSupplier.TypedData d : data) {
                if (d.isForceLiteral()) {
                    continue;
                }
                ElementType elementType = PlannerUtils.toElementType(d.type());
                try (Block.Builder builder = elementType.newBlockBuilder(positions, inputBlockFactory)) {
                    for (int p = 0; p < positions; p++) {
                        if (nullPositions.contains(p)) {
                            builder.appendNull();
                        } else {
                            builder.copyFrom(onePositionPage.getBlock(b), 0, 1);
                        }
                    }
                    manyPositionsBlocks[b] = builder.build();
                }
                b++;
            }
            try (
                ExpressionEvaluator eval = evaluator(expression).get(context);
                Block block = eval.eval(new Page(positions, manyPositionsBlocks))
            ) {
                for (int p = 0; p < positions; p++) {
                    if (nullPositions.contains(p)) {
                        assertThat(toJavaObject(block, p), allNullsMatcher());
                        continue;
                    }
                    assertThat(toJavaObjectUnsignedLongAware(block, p), testCase.getMatcher());
                }
                assertThat(
                    "evaluates to tracked block",
                    block.blockFactory(),
                    either(sameInstance(context.blockFactory())).or(sameInstance(inputBlockFactory))
                );
            }
        } finally {
            Releasables.close(onePositionPage::releaseBlocks, Releasables.wrap(manyPositionsBlocks));
        }
        if (testCase.getExpectedWarnings() != null) {
            assertWarnings(testCase.getExpectedWarnings());
        }
    }

    @Ignore("TODO: UNIGNORE AND FIX")
    public void testSimpleWithNulls() { // TODO replace this with nulls inserted into the test case like anyNullIsNull
        Expression expression = buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        List<Object> simpleData = testCase.getDataValues();
        try (ExpressionEvaluator eval = evaluator(expression).get(driverContext())) {
            BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
            Block[] orig = BlockUtils.fromListRow(blockFactory, simpleData);
            for (int i = 0; i < orig.length; i++) {
                List<Object> data = new ArrayList<>();
                Block[] blocks = new Block[orig.length];
                for (int b = 0; b < blocks.length; b++) {
                    if (b == i) {
                        blocks[b] = orig[b].elementType().newBlockBuilder(1, blockFactory).appendNull().build();
                        data.add(null);
                    } else {
                        blocks[b] = orig[b];
                        data.add(simpleData.get(b));
                    }
                }
                try (Block block = eval.eval(new Page(blocks))) {
                    assertSimpleWithNulls(data, block, i);
                }
            }

            // Note: the null-in-fast-null-out handling prevents any exception from being thrown, so the warnings provided in some test
            // cases won't actually be registered. This isn't an issue for unary functions, but could be an issue for n-ary ones, if
            // function processing of the first parameter(s) could raise an exception/warning. (But hasn't been the case so far.)
            // N-ary non-MV functions dealing with one multivalue (before hitting the null parameter injected above) will now trigger
            // a warning ("SV-function encountered a MV") that thus needs to be checked.
            if (true // this instanceof AbstractMultivalueFunctionTestCase == false
                && simpleData.stream().anyMatch(List.class::isInstance)
                && testCase.getExpectedWarnings() != null) {
                assertWarnings(testCase.getExpectedWarnings());
            }
        }
    }

    @Ignore("TODO: UNIGNORE AND FIX")
    public void testEvaluateInManyThreads() throws ExecutionException, InterruptedException {
        Expression expression = buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", EsqlDataTypes.isRepresentable(testCase.expectedType()));
        int count = 10_000;
        int threads = 5;
        var evalSupplier = evaluator(expression);
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                List<Object> simpleData = testCase.getDataValues();
                Page page = row(simpleData);

                futures.add(exec.submit(() -> {
                    try (ExpressionEvaluator eval = evalSupplier.get(driverContext())) {
                        for (int c = 0; c < count; c++) {
                            try (Block block = eval.eval(page)) {
                                assertThat(toJavaObjectUnsignedLongAware(block, 0), testCase.getMatcher());
                            }
                        }
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            exec.shutdown();
        }
    }

    @Ignore("TODO: UNIGNORE AND FIX")
    public void testEvaluatorToString() {
        Expression expression = buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        var factory = evaluator(expression);
        try (ExpressionEvaluator ev = factory.get(driverContext())) {
            assertThat(ev.toString(), testCase.evaluatorToString());
        }
    }

    @Ignore("TODO: UNIGNORE AND FIX")
    public void testFactoryToString() {
        Expression expression = buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        var factory = evaluator(buildFieldExpression(testCase));
        assertThat(factory.toString(), testCase.evaluatorToString());
    }

    @Ignore("TODO: Remove/Ignore/Separate from abstractFunction to avoid having this check")
    public void testFold() {
        Expression expression = buildLiteralExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assertFalse(expression.typeResolved().unresolved());
        Expression nullOptimized = new FoldNull().rule(expression);
        assertThat(nullOptimized.dataType(), equalTo(testCase.expectedType()));
        assertTrue(nullOptimized.foldable());
        if (testCase.foldingExceptionClass() == null) {
            Object result = nullOptimized.fold();
            // Decode unsigned longs into BigIntegers
            if (testCase.expectedType() == DataType.UNSIGNED_LONG && result != null) {
                result = NumericUtils.unsignedLongAsBigInteger((Long) result);
            }
            assertThat(result, testCase.getMatcher());
            if (testCase.getExpectedWarnings() != null) {
                assertWarnings(testCase.getExpectedWarnings());
            }
        } else {
            Throwable t = expectThrows(testCase.foldingExceptionClass(), nullOptimized::fold);
            assertThat(t.getMessage(), equalTo(testCase.foldingExceptionMessage()));
        }
    }

}
