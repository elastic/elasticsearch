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
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isSpatial;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
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

    public final void testEvaluate() {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        boolean readFloating = randomBoolean();
        Expression expression = readFloating ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Expected type must be representable to build an evaluator", EsqlDataTypes.isRepresentable(testCase.expectedType()));
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

    public void testSimpleWithNulls() { // TODO replace this with nulls inserted into the test case like anyNullIsNull
        Expression expression = buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        List<Object> simpleData = testCase.getDataValues();
        try (EvalOperator.ExpressionEvaluator eval = evaluator(expression).get(driverContext())) {
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
            if (this instanceof AbstractMultivalueFunctionTestCase == false
                && simpleData.stream().anyMatch(List.class::isInstance)
                && testCase.getExpectedWarnings() != null) {
                assertWarnings(testCase.getExpectedWarnings());
            }
        }
    }

    public final void testEvaluateInManyThreads() throws ExecutionException, InterruptedException {
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
                    try (EvalOperator.ExpressionEvaluator eval = evalSupplier.get(driverContext())) {
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

    /**
     * Adds cases with {@code null} and asserts that the result is {@code null}.
     * <p>
     * Note: This won't add more than a single null to any existing test case,
     * just to keep the number of test cases from exploding totally.
     * </p>
     *
     * @param entirelyNullPreservesType should a test case that only contains parameters
     *                                  with the {@code null} type keep it's expected type?
     *                                  This is <strong>mostly</strong> going to be {@code true}
     *                                  except for functions that base their type entirely
     *                                  on input types like {@link Greatest} or {@link Coalesce}.
     */
    protected static List<TestCaseSupplier> anyNullIsNull(boolean entirelyNullPreservesType, List<TestCaseSupplier> testCaseSuppliers) {
        return anyNullIsNull(
            testCaseSuppliers,
            (nullPosition, nullValueDataType, original) -> entirelyNullPreservesType == false
                && nullValueDataType == DataType.NULL
                && original.getData().size() == 1 ? DataType.NULL : original.expectedType(),
            (nullPosition, nullData, original) -> original
        );
    }

    public interface ExpectedType {
        DataType expectedType(int nullPosition, DataType nullValueDataType, TestCaseSupplier.TestCase original);
    }

    public interface ExpectedEvaluatorToString {
        Matcher<String> evaluatorToString(int nullPosition, TestCaseSupplier.TypedData nullData, Matcher<String> original);
    }

    protected static List<TestCaseSupplier> anyNullIsNull(
        List<TestCaseSupplier> testCaseSuppliers,
        ExpectedType expectedType,
        ExpectedEvaluatorToString evaluatorToString
    ) {
        typesRequired(testCaseSuppliers);
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers.size());
        suppliers.addAll(testCaseSuppliers);

        /*
         * For each original test case, add as many copies as there were
         * arguments, replacing one of the arguments with null and keeping
         * the others.
         *
         * Also, if this was the first time we saw the signature we copy it
         * *again*, replacing the argument with null, but annotating the
         * argument's type as `null` explicitly.
         */
        Set<List<DataType>> uniqueSignatures = new HashSet<>();
        for (TestCaseSupplier original : testCaseSuppliers) {
            boolean firstTimeSeenSignature = uniqueSignatures.add(original.types());
            for (int nullPosition = 0; nullPosition < original.types().size(); nullPosition++) {
                int finalNullPosition = nullPosition;
                suppliers.add(new TestCaseSupplier(original.name() + " null in " + nullPosition, original.types(), () -> {
                    TestCaseSupplier.TestCase oc = original.get();
                    List<TestCaseSupplier.TypedData> data = IntStream.range(0, oc.getData().size()).mapToObj(i -> {
                        TestCaseSupplier.TypedData od = oc.getData().get(i);
                        return i == finalNullPosition ? od.withData(null) : od;
                    }).toList();
                    TestCaseSupplier.TypedData nulledData = oc.getData().get(finalNullPosition);
                    return new TestCaseSupplier.TestCase(
                        data,
                        evaluatorToString.evaluatorToString(finalNullPosition, nulledData, oc.evaluatorToString()),
                        expectedType.expectedType(finalNullPosition, nulledData.type(), oc),
                        nullValue(),
                        null,
                        oc.getExpectedTypeError(),
                        null,
                        null
                    );
                }));

                if (firstTimeSeenSignature) {
                    List<DataType> typesWithNull = IntStream.range(0, original.types().size())
                        .mapToObj(i -> i == finalNullPosition ? DataType.NULL : original.types().get(i))
                        .toList();
                    boolean newSignature = uniqueSignatures.add(typesWithNull);
                    if (newSignature) {
                        suppliers.add(new TestCaseSupplier(typesWithNull, () -> {
                            TestCaseSupplier.TestCase oc = original.get();
                            List<TestCaseSupplier.TypedData> data = IntStream.range(0, oc.getData().size())
                                .mapToObj(i -> i == finalNullPosition ? TestCaseSupplier.TypedData.NULL : oc.getData().get(i))
                                .toList();
                            return new TestCaseSupplier.TestCase(
                                data,
                                equalTo("LiteralsEvaluator[lit=null]"),
                                expectedType.expectedType(finalNullPosition, DataType.NULL, oc),
                                nullValue(),
                                null,
                                oc.getExpectedTypeError(),
                                null,
                                null
                            );
                        }));
                    }
                }
            }
        }

        return suppliers;

    }

    /**
     * Adds test cases containing unsupported parameter types that assert
     * that they throw type errors.
     */
    protected static List<TestCaseSupplier> errorsForCasesWithoutExamples(
        List<TestCaseSupplier> testCaseSuppliers,
        PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        return errorsForCasesWithoutExamples(
            testCaseSuppliers,
            (i, v, t) -> AbstractScalarFunctionTestCase.typeErrorMessage(i, v, t, positionalErrorMessageSupplier)
        );
    }

    protected static List<TestCaseSupplier> errorsForCasesWithoutExamples(
        List<TestCaseSupplier> testCaseSuppliers,
        TypeErrorMessageSupplier typeErrorMessageSupplier
    ) {
        typesRequired(testCaseSuppliers);
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers.size());
        suppliers.addAll(testCaseSuppliers);

        Set<List<DataType>> valid = testCaseSuppliers.stream().map(TestCaseSupplier::types).collect(Collectors.toSet());
        List<Set<DataType>> validPerPosition = validPerPosition(valid);

        testCaseSuppliers.stream()
            .map(s -> s.types().size())
            .collect(Collectors.toSet())
            .stream()
            .flatMap(count -> allPermutations(count))
            .filter(types -> valid.contains(types) == false)
            /*
             * Skip any cases with more than one null. Our tests don't generate
             * the full combinatorial explosions of all nulls - just a single null.
             * Hopefully <null>, <null> cases will function the same as <null>, <valid>
             * cases.
             */.filter(types -> types.stream().filter(t -> t == DataType.NULL).count() <= 1)
            .map(types -> typeErrorSupplier(validPerPosition.size() != 1, validPerPosition, types, typeErrorMessageSupplier))
            .forEach(suppliers::add);
        return suppliers;
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
     * Validate that we know the types for all the test cases already created
     * @param suppliers - list of suppliers before adding in the illegal type combinations
     */
    private static void typesRequired(List<TestCaseSupplier> suppliers) {
        String bad = suppliers.stream().filter(s -> s.types() == null).map(s -> s.name()).collect(Collectors.joining("\n"));
        if (bad.equals("") == false) {
            throw new IllegalArgumentException("types required but not found for these tests:\n" + bad);
        }
    }

    private static List<Set<DataType>> validPerPosition(Set<List<DataType>> valid) {
        int max = valid.stream().mapToInt(List::size).max().getAsInt();
        List<Set<DataType>> result = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            result.add(new HashSet<>());
        }
        for (List<DataType> signature : valid) {
            for (int i = 0; i < signature.size(); i++) {
                result.get(i).add(signature.get(i));
            }
        }
        return result;
    }

    private static Stream<List<DataType>> allPermutations(int argumentCount) {
        if (argumentCount == 0) {
            return Stream.of(List.of());
        }
        if (argumentCount > 3) {
            throw new IllegalArgumentException("would generate too many combinations");
        }
        Stream<List<DataType>> stream = representable().map(t -> List.of(t));
        for (int i = 1; i < argumentCount; i++) {
            stream = stream.flatMap(types -> representable().map(t -> append(types, t)));
        }
        return stream;
    }

    private static List<DataType> append(List<DataType> orig, DataType extra) {
        List<DataType> longer = new ArrayList<>(orig.size() + 1);
        longer.addAll(orig);
        longer.add(extra);
        return longer;
    }

    @FunctionalInterface
    protected interface TypeErrorMessageSupplier {
        String apply(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types);
    }

    @FunctionalInterface
    protected interface PositionalErrorMessageSupplier {
        String apply(Set<DataType> validForPosition, int position);
    }

    protected static TestCaseSupplier typeErrorSupplier(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types,
        PositionalErrorMessageSupplier errorMessageSupplier
    ) {
        return typeErrorSupplier(
            includeOrdinal,
            validPerPosition,
            types,
            (o, v, t) -> AbstractScalarFunctionTestCase.typeErrorMessage(o, v, t, errorMessageSupplier)
        );
    }

    /**
     * Build a test case that asserts that the combination of parameter types is an error.
     */
    protected static TestCaseSupplier typeErrorSupplier(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types,
        TypeErrorMessageSupplier errorMessageSupplier
    ) {
        return new TestCaseSupplier(
            "type error for " + TestCaseSupplier.nameFromTypes(types),
            types,
            () -> TestCaseSupplier.TestCase.typeError(
                types.stream().map(type -> new TestCaseSupplier.TypedData(randomLiteral(type).value(), type, type.typeName())).toList(),
                errorMessageSupplier.apply(includeOrdinal, validPerPosition, types)
            )
        );
    }

    /**
     * Build the expected error message for an invalid type signature.
     */
    protected static String typeErrorMessage(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types,
        PositionalErrorMessageSupplier expectedTypeSupplier
    ) {
        int badArgPosition = -1;
        for (int i = 0; i < types.size(); i++) {
            if (validPerPosition.get(i).contains(types.get(i)) == false) {
                badArgPosition = i;
                break;
            }
        }
        if (badArgPosition == -1) {
            throw new IllegalStateException(
                "Can't generate error message for these types, you probably need a custom error message function"
            );
        }
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String expectedTypeString = expectedTypeSupplier.apply(validPerPosition.get(badArgPosition), badArgPosition);
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [] must be [" + expectedTypeString + "], found value [" + name + "] type [" + name + "]";
    }

    protected static Stream<DataType> representable() {
        return DataType.types().stream().filter(EsqlDataTypes::isRepresentable);
    }

    protected static DataType[] representableTypes() {
        return representable().toArray(DataType[]::new);
    }

    protected static Stream<DataType> representableNonSpatial() {
        return representable().filter(t -> isSpatial(t) == false);
    }

    protected static DataType[] representableNonSpatialTypes() {
        return representableNonSpatial().toArray(DataType[]::new);
    }
}
