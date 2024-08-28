/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Base class for scalar function tests. Tests based on this class will generally build out a single example evaluation,
 * which can be automatically tested against several scenarios (null handling, concurrency, etc).
 */
public abstract class AbstractScalarFunctionTestCase extends AbstractFunctionTestCase {

    /**
     * Converts a list of test cases into a list of parameter suppliers.
     * Also, adds a default set of extra test cases.
     * <p>
     *     Use if possible, as this method may get updated with new checks in the future.
     * </p>
     *
     * @param entirelyNullPreservesType See {@link #anyNullIsNull(boolean, List)}
     */
    protected static Iterable<Object[]> parameterSuppliersFromTypedDataWithDefaultChecks(
        boolean entirelyNullPreservesType,
        List<TestCaseSupplier> suppliers,
        PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(
                anyNullIsNull(entirelyNullPreservesType, randomizeBytesRefsOffset(suppliers)),
                positionalErrorMessageSupplier
            )
        );
    }

    /**
     * Converts a list of test cases into a list of parameter suppliers.
     * Also, adds a default set of extra test cases.
     * <p>
     *     Use if possible, as this method may get updated with new checks in the future.
     * </p>
     *
     * @param nullsExpectedType See {@link #anyNullIsNull(List, ExpectedType, ExpectedEvaluatorToString)}
     * @param evaluatorToString See {@link #anyNullIsNull(List, ExpectedType, ExpectedEvaluatorToString)}
     */
    protected static Iterable<Object[]> parameterSuppliersFromTypedDataWithDefaultChecks(
        ExpectedType nullsExpectedType,
        ExpectedEvaluatorToString evaluatorToString,
        List<TestCaseSupplier> suppliers,
        PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(
                anyNullIsNull(randomizeBytesRefsOffset(suppliers), nullsExpectedType, evaluatorToString),
                positionalErrorMessageSupplier
            )
        );
    }

    public final void testEvaluate() {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        boolean readFloating = randomBoolean();
        Expression expression = readFloating ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Expected type must be representable to build an evaluator", DataType.isRepresentable(testCase.expectedType()));
        logger.info(
            "Test Values: " + testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","))
        );
        Expression.TypeResolution resolution = expression.typeResolved();
        if (resolution.unresolved()) {
            throw new AssertionError("expected resolved " + resolution.message());
        }
        expression = new FoldNull().rule(expression);
        assertThat(expression.dataType(), equalTo(testCase.expectedType()));
        logger.info("Result type: " + expression.dataType());

        Object result;
        try (ExpressionEvaluator evaluator = evaluator(expression).get(driverContext())) {
            try (Block block = evaluator.eval(row(testCase.getDataValues()))) {
                assertThat(block.getPositionCount(), is(1));
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

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern..
     * <p>
     * Note that this'll sometimes be a {@link Vector} of values if the
     * input pattern contained only a single value.
     * </p>
     */
    public final void testEvaluateBlockWithoutNulls() {
        assumeTrue("no warning is expected", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), driverContext(), false);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
            fail("Test data is too large to fit in the memory");
        }
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between.
     */
    public final void testEvaluateBlockWithNulls() {
        assumeTrue("no warning is expected", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), driverContext(), true);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
            fail("Test data is too large to fit in the memory");
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
    public final void testCrankyEvaluateBlockWithoutNulls() {
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
    public final void testCrankyEvaluateBlockWithNulls() {
        assumeTrue("sometimes the cranky breaker silences warnings, just skip these cases", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), crankyContext(), true);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    protected Matcher<Object> allNullsMatcher() {
        return nullValue();
    }

    private void testEvaluateBlock(BlockFactory inputBlockFactory, DriverContext context, boolean insertNulls) {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", DataType.isRepresentable(testCase.expectedType()));
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
                assertThat(block.getPositionCount(), is(positions));
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

    public final void testEvaluateInManyThreads() throws ExecutionException, InterruptedException {
        Expression expression = buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", DataType.isRepresentable(testCase.expectedType()));
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
                    try (EvalOperator.ExpressionEvaluator eval = evalSupplier.get(driverContext())) {
                        for (int c = 0; c < count; c++) {
                            try (Block block = eval.eval(page)) {
                                assertThat(block.getPositionCount(), is(1));
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

    public final void testEvaluatorToString() {
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

    public final void testFactoryToString() {
        Expression expression = buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        var factory = evaluator(buildFieldExpression(testCase));
        assertThat(factory.toString(), testCase.evaluatorToString());
    }

    public final void testFold() {
        Expression expression = buildLiteralExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assertFalse("expected resolved", expression.typeResolved().unresolved());
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

    public static String errorMessageStringForBinaryOperators(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types,
        PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        try {
            return typeErrorMessage(includeOrdinal, validPerPosition, types, positionalErrorMessageSupplier);
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is from the combination
            if (types.get(0).equals(DataType.UNSIGNED_LONG)) {
                return "first argument of [] is [unsigned_long] and second is ["
                    + types.get(1).typeName()
                    + "]. [unsigned_long] can only be operated on together with another [unsigned_long]";

            }
            if (types.get(1).equals(DataType.UNSIGNED_LONG)) {
                return "first argument of [] is ["
                    + types.get(0).typeName()
                    + "] and second is [unsigned_long]. [unsigned_long] can only be operated on together with another [unsigned_long]";
            }
            return "first argument of [] is ["
                + (types.get(0).isNumeric() ? "numeric" : types.get(0).typeName())
                + "] so second argument must also be ["
                + (types.get(0).isNumeric() ? "numeric" : types.get(0).typeName())
                + "] but was ["
                + types.get(1).typeName()
                + "]";

        }
    }

    /**
     * Adds test cases containing unsupported parameter types that immediately fail.
     */
    protected static List<TestCaseSupplier> failureForCasesWithoutExamples(List<TestCaseSupplier> testCaseSuppliers) {
        typesRequired(testCaseSuppliers);
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers.size());
        suppliers.addAll(testCaseSuppliers);

        Set<List<DataType>> valid = testCaseSuppliers.stream().map(TestCaseSupplier::types).collect(Collectors.toSet());

        testCaseSuppliers.stream()
            .map(s -> s.types().size())
            .collect(Collectors.toSet())
            .stream()
            .flatMap(count -> allPermutations(count))
            .filter(types -> valid.contains(types) == false)
            .map(types -> new TestCaseSupplier("type error for " + TestCaseSupplier.nameFromTypes(types), types, () -> {
                throw new IllegalStateException("must implement a case for " + types);
            }))
            .forEach(suppliers::add);
        return suppliers;
    }

    /**
     * Build a test case checking for arithmetic overflow.
     */
    protected static TestCaseSupplier arithmeticExceptionOverflowCase(
        DataType dataType,
        Supplier<Object> lhsSupplier,
        Supplier<Object> rhsSupplier,
        String evaluator
    ) {
        String typeNameOverflow = dataType.typeName().toLowerCase(Locale.ROOT) + " overflow";
        return new TestCaseSupplier(
            "<" + typeNameOverflow + ">",
            List.of(dataType),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhsSupplier.get(), dataType, "lhs"),
                    new TestCaseSupplier.TypedData(rhsSupplier.get(), dataType, "rhs")
                ),
                evaluator + "[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                dataType,
                is(nullValue())
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.ArithmeticException: " + typeNameOverflow)
        );
    }
}
