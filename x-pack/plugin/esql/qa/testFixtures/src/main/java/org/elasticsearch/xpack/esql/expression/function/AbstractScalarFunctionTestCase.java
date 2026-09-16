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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.FilterUnsupportedTemporality;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPSeriesWeightedSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvRLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvUnion;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvZip;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.vector.Magnitude;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
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
     * Scalar expressions that deliberately don't propagate null through all of their arguments,
     * and therefore don't implement {@link AnyNullIsNull}.
     * <p>
     * Every scalar function must either implement {@link AnyNullIsNull} or be registered here.
     * This forces a conscious decision whenever a new scalar function is added.
     */
    private static final Set<Class<? extends Expression>> EXPRESSIONS_WITHOUT_ANY_NULL_IS_NULL = Set.of(
        // Three-valued-logic expressions, that return non-null for some null inputs.
        Case.class, // CASE(NULL, 1, 2) = 2
        Coalesce.class, // COALESCE(NULL, 1) = 1
        IsNotNull.class, // NULL IS NOT NULL = false
        IsNull.class, // NULL IS NULL = true

        // Multivalue functions that treat NULL is an empty set.
        MvContains.class, // MV_CONTAINS([1, 2], NULL) = false
        MvDifference.class, // MV_DIFFERENCE([1, 2], NULL) = [1, 2]
        MvGreater.class, // MV_GREATER(NULL, 1) = false
        MvInRange.class, // MV_IN_RANGE(NULL, 1, 2) = false
        MvIntersects.class, // MV_INTERSECTS([1, 2], NULL) = false
        MvLess.class, // MV_LESS(NULL, 1) = false
        MvLike.class, // MV_LIKE(NULL, "a*") = false
        MvRLike.class, // MV_RLIKE(NULL, "a.*") = false
        MvUnion.class, // MV_UNION(NULL, [1]) = [1]
        MvZip.class, // MV_ZIP(NULL, ["a"], ",") = ["a"]

        // Special empty/null handling functions
        Magnitude.class, // MAGNITUDE(<all-null>) = 0-length block, not null
        MvPSeriesWeightedSum.class, // MV_PSERIES_WEIGHTED_SUM(NULL, 2) = 0.0

        // Non-evaluatable grouping functions
        Categorize.class,
        TimeSeriesWithout.class,

        // Explicitly null-tolerant evaluator: @Evaluator(allNullsIsNull = false).
        FilterUnsupportedTemporality.class
    );

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
        List<TestCaseSupplier> suppliers
    ) {
        return parameterSuppliersFromTypedData(anyNullIsNull(entirelyNullPreservesType, randomizeBytesRefsOffset(suppliers)));
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
        List<TestCaseSupplier> suppliers
    ) {
        return parameterSuppliersFromTypedData(anyNullIsNull(randomizeBytesRefsOffset(suppliers), nullsExpectedType, evaluatorToString));
    }

    public final void testEvaluate() {
        testEvaluate(false);
    }

    /**
     * @param testAnyNullIsNull If false, execute the testcase as is
     *                          If true, if the testcase does not contain any nulls,
     *                          replace a random entry with null, and assert the result is null
     */
    private void testEvaluate(boolean testAnyNullIsNull) {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        logger.info(
            "Test Values: " + testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","))
        );
        Expression.TypeResolution resolution = expression.typeResolved();
        if (resolution.unresolved()) {
            throw new AssertionError("expected resolved " + resolution.message());
        }
        expression = new FoldNull().rule(expression, unboundLogicalOptimizerContext());
        assertThat("Expression yielded unexpected datatype", expression.dataType(), equalTo(testCase.expectedType()));
        logger.info("Result type: " + expression.dataType());

        Object result;
        try (ExpressionEvaluator evaluator = evaluator(expression).get(driverContext())) {
            if (testCase.getExpectedBuildEvaluatorWarnings() != null) {
                assertWarnings(testCase.getExpectedBuildEvaluatorWarnings());
            }
            List<Object> dataValues = testCase.getDataValues();
            if (testAnyNullIsNull && dataValues.stream().noneMatch(Objects::isNull)) {
                int randomIndex = randomInt(dataValues.size() - 1);
                logger.info("testAnyNullIsNull: setting index " + randomIndex + " to null");
                dataValues.set(randomIndex, null);
            }
            Page row = row(dataValues);
            try (Block block = evaluator.eval(row)) {
                assertThat(block.getPositionCount(), is(1));
                result = toJavaObject(block, 0);
                if (testAnyNullIsNull == false) {
                    extraBlockTests(row, block);
                }
            } finally {
                row.releaseBlocks();
            }
        }
        if (testAnyNullIsNull) {
            assertThat(result, nullValue());
        } else {
            assertTestCaseResultAndWarnings(result);
        }
    }

    /**
     * Functions marked with {@link AnyNullIsNull} promise to return {@code null} whenever any of their
     * (non-constant) arguments is {@code null}. This verifies that contract automatically for every such
     * function.
     * <p>
     * This also tests that any scalar function is either marked with {@link AnyNullIsNull} or is on
     * the list {@link #EXPRESSIONS_WITHOUT_ANY_NULL_IS_NULL}.
     */
    public final void testAnyNullIsNull() {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("No warnings expected", testCase.getExpectedWarnings() == null);

        Expression expression = buildFieldExpression(testCase);
        assertThat(
            expression.getClass().getName()
                + " must implement "
                + AnyNullIsNull.class.getSimpleName()
                + " or be registered in EXPRESSIONS_WITHOUT_ANY_NULL_IS_NULL",
            expression instanceof AnyNullIsNull ^ EXPRESSIONS_WITHOUT_ANY_NULL_IS_NULL.contains(expression.getClass()),
            is(true)
        );

        assumeTrue("Function is not marked " + AnyNullIsNull.class.getSimpleName(), expression instanceof AnyNullIsNull);

        if (testCase.getDataValues().isEmpty() == false) {
            testEvaluate(true);
        }
    }

    /**
     * Extra assertions on the output block.
     */
    protected void extraBlockTests(Page in, Block out) {}

    protected final void assertIsOrdIfInIsOrd(Page in, Block out) {
        BytesRefBlock inBytes = in.getBlock(0);
        BytesRefBlock outBytes = (BytesRefBlock) out;

        BytesRefVector inVec = inBytes.asVector();
        if (inVec == null) {
            assertThat(outBytes.asVector(), nullValue());
            return;
        }
        BytesRefVector outVec = outBytes.asVector();

        if (inVec.isConstant()) {
            assertTrue(outVec.isConstant());
            return;
        }

        if (inVec.asOrdinals() != null) {
            assertThat(outBytes.asOrdinals(), not(nullValue()));
            return;
        }
        assertThat(outBytes.asOrdinals(), nullValue());
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
        assumeTrue(
            "sometimes the cranky breaker silences warnings, just skip these cases",
            testCase.getExpectedBuildEvaluatorWarnings() == null
        );
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
        assumeTrue(
            "sometimes the cranky breaker silences warnings, just skip these cases",
            testCase.getExpectedBuildEvaluatorWarnings() == null
        );
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
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
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
            Page in = new Page(positions, manyPositionsBlocks);
            try (ExpressionEvaluator eval = evaluator(expression).get(context); Block block = eval.eval(in)) {
                if (testCase.getExpectedBuildEvaluatorWarnings() != null) {
                    assertWarnings(testCase.getExpectedBuildEvaluatorWarnings());
                }
                assertThat("Unexpected number of positions", block.getPositionCount(), is(positions));
                for (int p = 0; p < positions; p++) {
                    if (nullPositions.contains(p)) {
                        assertThat(toJavaObjectUnsignedLongAware(block, p), allNullsMatcher());
                        continue;
                    }
                    assertThat("Unexpected value at position '" + p + "'", toJavaObjectUnsignedLongAware(block, p), testCase.getMatcher());
                }
                assertThat(
                    "evaluates to tracked block",
                    block.blockFactory(),
                    either(sameInstance(context.blockFactory())).or(sameInstance(inputBlockFactory))
                );
                extraBlockTests(in, block);
            }
        } finally {
            Releasables.close(onePositionPage::releaseBlocks, Releasables.wrap(manyPositionsBlocks));
        }
        if (testCase.getExpectedWarnings() != null) {
            assertWarnings(testCase.getExpectedWarnings());
        }
    }

    private int evaluateInManyThreadsCountMax() {
        for (TestCaseSupplier.TypedData dt : testCase.getData()) {
            if (dt.type() == DataType.EXPONENTIAL_HISTOGRAM) {
                return 500;
            }
        }
        return 10_000;
    }

    public final void testEvaluateInManyThreads() throws ExecutionException, InterruptedException {
        Expression expression = buildFieldExpression(testCase);
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        int count = scaledRandomIntBetween(100, evaluateInManyThreadsCountMax());
        int threads = 5;
        var evalSupplier = evaluator(expression);
        if (testCase.getExpectedBuildEvaluatorWarnings() != null) {
            assertWarnings(testCase.getExpectedBuildEvaluatorWarnings());
        }

        try (ExecutorService exec = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                List<Object> simpleData = testCase.getDataValues();
                Page page = row(simpleData);

                futures.add(exec.submit(() -> {
                    try (ExpressionEvaluator eval = evalSupplier.get(driverContext())) {
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
        }
        // This test exercises thread-safety, not warning content: each thread accumulates (possibly duplicated)
        // warnings into its own per-driver sink. Consume them so the leak-check passes, asserting only that nothing
        // unexpected surfaced.
        consumeAndAssertExpectedDriverWarnings();
    }

    public final void testEvaluatorToString() {
        Expression expression = buildFieldExpression(testCase);
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        var factory = evaluator(expression);
        try (ExpressionEvaluator ev = factory.get(driverContext())) {
            if (testCase.getExpectedBuildEvaluatorWarnings() != null) {
                assertWarnings(testCase.getExpectedBuildEvaluatorWarnings());
            }
            assertThat(ev.toString(), testCase.evaluatorToString());
        }
    }

    public final void testFactoryToString() {
        Expression expression = buildFieldExpression(testCase);
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        var factory = evaluator(expression);
        if (testCase.getExpectedBuildEvaluatorWarnings() != null) {
            assertWarnings(testCase.getExpectedBuildEvaluatorWarnings());
        }
        assertThat(factory.toString(), testCase.evaluatorToString());
    }

    public void testFold() {
        Expression expression = buildLiteralExpression(testCase);
        assertFalse("expected resolved", expression.typeResolved().unresolved());
        if (expression instanceof SurrogateExpression s) {
            Expression surrogate = s.surrogate();
            if (surrogate != null) {
                expression = surrogate;
            }
        }
        Expression nullOptimized = new FoldNull().rule(expression, unboundLogicalOptimizerContext());
        assertThat(nullOptimized.dataType(), equalTo(testCase.expectedType()));
        assertTrue(nullOptimized.foldable());
        if (testCase.foldingExceptionClass() == null) {
            Object result = nullOptimized.fold(FoldContext.small());
            // Decode unsigned longs into BigIntegers
            if (testCase.expectedType() == DataType.UNSIGNED_LONG && result != null) {
                if (result instanceof List<?> l) {
                    result = l.stream().map(v -> NumericUtils.unsignedLongAsBigInteger((Long) v)).toList();
                } else {
                    result = NumericUtils.unsignedLongAsBigInteger((Long) result);
                }
            }
            assertThat(result, testCase.getMatcher());
            if (testCase.getExpectedBuildEvaluatorWarnings() != null) {
                assertWarnings(testCase.getExpectedBuildEvaluatorWarnings());
            }
            if (testCase.getExpectedWarnings() != null) {
                // Fold-time warnings go to HTTP response headers (plan-time channel). Functions that
                // emit warnings during constant folding must use HeaderWarning.addWarning(...) so that
                // assertWarnings() can find them here.
                assertWarnings(testCase.getExpectedWarnings());
            }
        } else {
            Throwable t = expectThrows(testCase.foldingExceptionClass(), () -> nullOptimized.fold(FoldContext.small()));
            assertThat(t.getMessage(), equalTo(testCase.foldingExceptionMessage()));
        }
    }

    /**
     * Adds test cases containing unsupported parameter types that immediately fail.
     */
    protected static List<TestCaseSupplier> failureForCasesWithoutExamples(List<TestCaseSupplier> testCaseSuppliers) {
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers.size());
        suppliers.addAll(testCaseSuppliers);

        Set<List<DataType>> valid = testCaseSuppliers.stream().map(TestCaseSupplier::types).collect(Collectors.toSet());

        testCaseSuppliers.stream()
            .map(s -> s.types().size())
            .collect(Collectors.toSet())
            .stream()
            .flatMap(AbstractFunctionTestCase::allPermutations)
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
            List.of(dataType, dataType),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhsSupplier.get(), dataType, "lhs"),
                    new TestCaseSupplier.TypedData(rhsSupplier.get(), dataType, "rhs")
                ),
                evaluator + "[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                dataType,
                is(nullValue())
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.ArithmeticException: " + typeNameOverflow)
        );
    }
}
