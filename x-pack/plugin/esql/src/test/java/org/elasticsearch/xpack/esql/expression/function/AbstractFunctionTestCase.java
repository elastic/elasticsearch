/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Base class for function tests.  Tests based on this class will generally build out a single example evaluation,
 * which can be automatically tested against several scenarios (null handling, concurrency, etc).
 */
public abstract class AbstractFunctionTestCase extends ESTestCase {
    /**
     * Generate a random value of the appropriate type to fit into blocks of {@code e}.
     */
    public static Literal randomLiteral(DataType type) {
        return new Literal(Source.EMPTY, switch (type.typeName()) {
            case "boolean" -> randomBoolean();
            case "byte" -> randomByte();
            case "short" -> randomShort();
            case "integer" -> randomInt();
            case "unsigned_long", "long" -> randomLong();
            case "date_period" -> Period.of(randomIntBetween(-1000, 1000), randomIntBetween(-13, 13), randomIntBetween(-32, 32));
            case "datetime" -> randomMillisUpToYear9999();
            case "double", "scaled_float" -> randomDouble();
            case "float" -> randomFloat();
            case "half_float" -> HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
            case "keyword" -> new BytesRef(randomAlphaOfLength(5));
            case "ip" -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
            case "time_duration" -> Duration.ofMillis(randomLongBetween(-604800000L, 604800000L)); // plus/minus 7 days
            case "text" -> new BytesRef(randomAlphaOfLength(50));
            case "version" -> randomVersion().toBytesRef();
            case "null" -> null;
            case "_source" -> {
                try {
                    yield BytesReference.bytes(
                        JsonXContent.contentBuilder().startObject().field(randomAlphaOfLength(3), randomAlphaOfLength(10)).endObject()
                    ).toBytesRef();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            default -> throw new IllegalArgumentException("can't make random values for [" + type.typeName() + "]");
        }, type);
    }

    protected TestCaseSupplier.TestCase testCase;

    protected static Iterable<Object[]> parameterSuppliersFromTypedData(List<TestCaseSupplier> suppliers) {
        // TODO rename this method to something more descriptive. Javadoc. And make sure all parameters are "representable" types.
        List<Object[]> parameters = new ArrayList<>(suppliers.size());
        for (TestCaseSupplier supplier : suppliers) {
            parameters.add(new Object[] { supplier });
        }
        return parameters;
    }

    protected static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true));
    }

    protected static Expression deepCopyOfField(String name, DataType type) {
        return new DeepCopy(Source.EMPTY, new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true)));
    }

    /**
     * Build the expression being tested, for the given source and list of arguments.  Test classes need to implement this
     * to have something to test.
     * @param source the source
     * @param args arg list from the test case, should match the length expected
     * @return an expression for evaluating the function being tested on the given arguments
     */
    protected abstract Expression build(Source source, List<Expression> args);

    protected final Expression buildFieldExpression(TestCaseSupplier.TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsFields());
    }

    protected final Expression buildDeepCopyOfFieldExpression(TestCaseSupplier.TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsDeepCopiedFields());
    }

    protected final Expression buildLiteralExpression(TestCaseSupplier.TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsLiterals());
    }

    protected final ExpressionEvaluator.Factory evaluator(Expression e) {
        e = new FoldNull().rule(e);
        if (e.foldable()) {
            e = new Literal(e.source(), e.fold(), e.dataType());
        }
        Layout.Builder builder = new Layout.Builder();
        buildLayout(builder, e);
        assertTrue(e.resolved());
        return EvalMapper.toEvaluator(e, builder.build());
    }

    protected final Page row(List<Object> values) {
        return new Page(BlockUtils.fromListRow(BlockFactory.getNonBreakingInstance(), values));
    }

    /**
     * Hack together a layout by scanning for Fields.
     * Those will show up in the layout in whatever order a depth first traversal finds them.
     */
    protected void buildLayout(Layout.Builder builder, Expression e) {
        if (e instanceof FieldAttribute f) {
            builder.append(f);
            return;
        }
        for (Expression c : e.children()) {
            buildLayout(builder, c);
        }
    }

    protected final void assertResolveTypeValid(Expression expression, DataType expectedType) {
        assertTrue(expression.typeResolved().resolved());
        assertThat(expression.dataType(), equalTo(expectedType));
    }

    public final void testEvaluate() {
        testEvaluate(false);
    }

    public final void testEvaluateFloating() {
        testEvaluate(true);
    }

    private void testEvaluate(boolean readFloating) {
        assumeTrue("All test data types must be representable in order to build fields", testCase.allTypesAreRepresentable());
        logger.info(
            "Test Values: " + testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","))
        );
        Expression expression = readFloating ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTrue("expected unresolved", expression.typeResolved().unresolved());
            if (readFloating == false) {
                // The hack that creates floating fields changes the error message so don't assert it
                assertThat(expression.typeResolved().message(), equalTo(testCase.getExpectedTypeError()));
            }
            return;
        }
        assertFalse("expected resolved", expression.typeResolved().unresolved());
        expression = new FoldNull().rule(expression);
        assertThat(expression.dataType(), equalTo(testCase.expectedType));
        // TODO should we convert unsigned_long into BigDecimal so it's easier to assert?
        Object result;
        try (ExpressionEvaluator evaluator = evaluator(expression).get(driverContext())) {
            try (Block block = evaluator.eval(row(testCase.getDataValues()))) {
                result = toJavaObject(block, 0);
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
     * Evaluates a {@link Block} of values, all copied from the input pattern, read directly from the page.
     * <p>
     *     Note that this'll sometimes be a {@link Vector} of values if the
     *     input pattern contained only a single value.
     * </p>
     */
    public final void testEvaluateBlockWithoutNulls() {
        testEvaluateBlock(driverContext().blockFactory(), driverContext(), false, false);
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern, read from an intermediate operator.
     * <p>
     *     Note that this'll sometimes be a {@link Vector} of values if the
     *     input pattern contained only a single value.
     * </p>
     */
    public final void testEvaluateBlockWithoutNullsFloating() {
        testEvaluateBlock(driverContext().blockFactory(), driverContext(), false, true);
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between, read directly from the page.
     */
    public final void testEvaluateBlockWithNulls() {
        testEvaluateBlock(driverContext().blockFactory(), driverContext(), true, false);
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between, read from an intermediate operator.
     */
    public final void testEvaluateBlockWithNullsFloating() {
        testEvaluateBlock(driverContext().blockFactory(), driverContext(), true, true);
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern,
     * read directly from the {@link Page}, using the
     * {@link CrankyCircuitBreakerService} which fails randomly.
     * <p>
     *     Note that this'll sometimes be a {@link Vector} of values if the
     *     input pattern contained only a single value.
     * </p>
     */
    public final void testCrankyEvaluateBlockWithoutNulls() {
        assumeTrue("sometimes the cranky breaker silences warnings, just skip these cases", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), crankyContext(), false, false);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern,
     * read from an intermediate operator, using the
     * {@link CrankyCircuitBreakerService} which fails randomly.
     * <p>
     *     Note that this'll sometimes be a {@link Vector} of values if the
     *     input pattern contained only a single value.
     * </p>
     */
    public final void testCrankyEvaluateBlockWithoutNullsFloating() {
        assumeTrue("sometimes the cranky breaker silences warnings, just skip these cases", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), crankyContext(), false, true);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between, read directly from the page,
     * using the {@link CrankyCircuitBreakerService} which fails randomly.
     */
    public final void testCrankyEvaluateBlockWithNulls() {
        assumeTrue("sometimes the cranky breaker silences warnings, just skip these cases", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), crankyContext(), true, false);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    /**
     * Evaluates a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between, read from an intermediate operator,
     * using the {@link CrankyCircuitBreakerService} which fails randomly.
     */
    public final void testCrankyEvaluateBlockWithNullsFloating() {
        assumeTrue("sometimes the cranky breaker silences warnings, just skip these cases", testCase.getExpectedWarnings() == null);
        try {
            testEvaluateBlock(driverContext().blockFactory(), crankyContext(), true, true);
        } catch (CircuitBreakingException ex) {
            assertThat(ex.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    /**
     * Does the function produce the same output regardless of input?
     */
    protected Matcher<Object> allNullsMatcher() {
        return nullValue();
    }

    private void testEvaluateBlock(BlockFactory inputBlockFactory, DriverContext context, boolean insertNulls, boolean readFloating) {
        assumeTrue("can only run on representable types", testCase.allTypesAreRepresentable());
        assumeTrue("must build evaluator to test sending it blocks", testCase.getExpectedTypeError() == null);
        int positions = between(1, 1024);
        List<TestCaseSupplier.TypedData> data = testCase.getData();
        Page onePositionPage = row(testCase.getDataValues());
        Block[] manyPositionsBlocks = new Block[data.size()];
        Set<Integer> nullPositions = insertNulls
            ? IntStream.range(0, positions).filter(i -> randomBoolean()).mapToObj(Integer::valueOf).collect(Collectors.toSet())
            : Set.of();
        if (nullPositions.size() == positions) {
            nullPositions = Set.of();
        }
        try {
            for (int b = 0; b < data.size(); b++) {
                ElementType elementType = LocalExecutionPlanner.toElementType(data.get(b).type());
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
            }
            Expression expression = readFloating ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
            try (ExpressionEvaluator eval = evaluator(expression).get(context); Block block = eval.eval(new Page(manyPositionsBlocks))) {
                for (int p = 0; p < positions; p++) {
                    if (nullPositions.contains(p)) {
                        assertThat(toJavaObject(block, p), allNullsMatcher());
                        continue;
                    }
                    assertThat(toJavaObject(block, p), testCase.getMatcher());
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

    // TODO cranky time

    public final void testSimpleWithNulls() { // TODO replace this with nulls inserted into the test case like anyNullIsNull
        assumeTrue("nothing to do if a type error", testCase.getExpectedTypeError() == null);
        assumeTrue("All test data types must be representable in order to build fields", testCase.allTypesAreRepresentable());
        List<Object> simpleData = testCase.getDataValues();
        try (EvalOperator.ExpressionEvaluator eval = evaluator(buildFieldExpression(testCase)).get(driverContext())) {
            Block[] orig = BlockUtils.fromListRow(BlockFactory.getNonBreakingInstance(), simpleData);
            for (int i = 0; i < orig.length; i++) {
                List<Object> data = new ArrayList<>();
                Block[] blocks = new Block[orig.length];
                for (int b = 0; b < blocks.length; b++) {
                    if (b == i) {
                        blocks[b] = orig[b].elementType().newBlockBuilder(1).appendNull().build();
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
        }
    }

    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        // TODO remove me in favor of cases containing null
        assertTrue("argument " + nullBlock + " is null", value.isNull(0));
    }

    public final void testEvaluateInManyThreads() throws ExecutionException, InterruptedException {
        assumeTrue("nothing to do if a type error", testCase.getExpectedTypeError() == null);
        assumeTrue("All test data types must be representable in order to build fields", testCase.allTypesAreRepresentable());
        int count = 10_000;
        int threads = 5;
        var evalSupplier = evaluator(buildFieldExpression(testCase));
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
                                assertThat(toJavaObject(block, 0), testCase.getMatcher());
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
        assumeTrue("nothing to do if a type error", testCase.getExpectedTypeError() == null);
        assumeTrue("All test data types must be representable in order to build fields", testCase.allTypesAreRepresentable());
        var factory = evaluator(buildFieldExpression(testCase));
        try (ExpressionEvaluator ev = factory.get(driverContext())) {
            assertThat(ev.toString(), equalTo(testCase.evaluatorToString));
        }
    }

    public final void testFactoryToString() {
        assumeTrue("nothing to do if a type error", testCase.getExpectedTypeError() == null);
        assumeTrue("All test data types must be representable in order to build fields", testCase.allTypesAreRepresentable());
        var factory = evaluator(buildFieldExpression(testCase));
        assertThat(factory.toString(), equalTo(testCase.evaluatorToString));
    }

    public final void testFold() {
        Expression expression = buildLiteralExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTrue(expression.typeResolved().unresolved());
            assertThat(expression.typeResolved().message(), equalTo(testCase.getExpectedTypeError()));
            return;
        }
        assertFalse(expression.typeResolved().unresolved());
        expression = new FoldNull().rule(expression);
        assertThat(expression.dataType(), equalTo(testCase.expectedType));
        assertTrue(expression.foldable());
        assertThat(expression.fold(), testCase.getMatcher());
        if (testCase.getExpectedWarnings() != null) {
            assertWarnings(testCase.getExpectedWarnings());
        }
    }

    public void testSerializationOfSimple() {
        assumeTrue("All test data types must be representable in order to build fields", testCase.allTypesAreRepresentable());
        assertSerialization(buildFieldExpression(testCase));
    }

    private static boolean ranAllTests = false;

    @ClassRule
    public static TestRule rule = new TestRule() {
        @Override
        public Statement apply(Statement base, Description description) {
            for (Description d : description.getChildren()) {
                if (d.getChildren().size() > 1) {
                    ranAllTests = true;
                    return base;
                }
            }
            return base;
        }
    };

    @AfterClass
    public static void testFunctionInfo() {
        if (ranAllTests == false) {
            LogManager.getLogger(getTestClass()).info("Skipping function info checks because we're running a portion of the tests");
            return;
        }
        FunctionDefinition definition = definition();
        if (definition == null) {
            LogManager.getLogger(getTestClass()).info("Skipping function info checks because the function isn't registered");
            return;
        }
        LogManager.getLogger(getTestClass()).info("Running function info checks");
        EsqlFunctionRegistry.FunctionDescription description = EsqlFunctionRegistry.description(definition);
        List<EsqlFunctionRegistry.ArgSignature> args = description.args();

        List<Set<String>> typesFromSignature = new ArrayList<>();
        Set<String> returnFromSignature = new HashSet<>();
        for (int i = 0; i < args.size(); i++) {
            typesFromSignature.add(new HashSet<>());
        }
        for (Map.Entry<List<DataType>, DataType> entry : signatures.entrySet()) {
            List<DataType> types = entry.getKey();
            for (int i = 0; i < args.size() && i < types.size(); i++) {
                typesFromSignature.get(i).add(types.get(i).esType());
            }
            returnFromSignature.add(entry.getValue().esType());
        }

        for (int i = 0; i < args.size(); i++) {
            Set<String> annotationTypes = Arrays.stream(args.get(i).type()).collect(Collectors.toCollection(() -> new TreeSet<>()));
            if (annotationTypes.equals(Set.of("?"))) {
                continue; // TODO remove this eventually, so that all the functions will have to provide signature info
            }
            Set<String> signatureTypes = typesFromSignature.get(i);
            if (signatureTypes.isEmpty()) {
                continue;
            }
            assertEquals(annotationTypes, signatureTypes);
        }

        Set<String> returnTypes = Arrays.stream(description.returnType()).collect(Collectors.toCollection(() -> new TreeSet<>()));
        if (returnTypes.equals(Set.of("?")) == false) {
            // TODO remove this eventually, so that all the functions will have to provide signature info
            assertEquals(returnTypes, returnFromSignature);
        }
    }

    /**
     * Adds cases with {@code null} and asserts that the result is {@code null}.
     * <p>
     *     Note: This won't add more than a single null to any existing test case,
     *     just to keep the number of test cases from exploding totally.
     * </p>
     * @param  entirelyNullPreservesType should a test case that only contains parameters
     *                                   with the {@code null} type keep it's expected type?
     *                                   This is <strong>mostly</strong> going to be {@code true}
     *                                   except for functions that base their type entirely
     *                                   on input types like {@link Greatest} or {@link Coalesce}.
     */
    protected static List<TestCaseSupplier> anyNullIsNull(boolean entirelyNullPreservesType, List<TestCaseSupplier> testCaseSuppliers) {
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
                        if (i == finalNullPosition) {
                            return new TestCaseSupplier.TypedData(null, od.type(), od.name());
                        }
                        return od;
                    }).toList();
                    return new TestCaseSupplier.TestCase(
                        data,
                        oc.evaluatorToString,
                        oc.expectedType,
                        nullValue(),
                        null,
                        oc.getExpectedTypeError()
                    );
                }));

                if (firstTimeSeenSignature) {
                    List<DataType> typesWithNull = IntStream.range(0, original.types().size())
                        .mapToObj(i -> i == finalNullPosition ? DataTypes.NULL : original.types().get(i))
                        .toList();
                    boolean newSignature = uniqueSignatures.add(typesWithNull);
                    if (newSignature) {
                        suppliers.add(new TestCaseSupplier(typesWithNull, () -> {
                            TestCaseSupplier.TestCase oc = original.get();
                            List<TestCaseSupplier.TypedData> data = IntStream.range(0, oc.getData().size()).mapToObj(i -> {
                                TestCaseSupplier.TypedData od = oc.getData().get(i);
                                if (i == finalNullPosition) {
                                    return new TestCaseSupplier.TypedData(null, DataTypes.NULL, od.name());
                                }
                                return od;
                            }).toList();
                            return new TestCaseSupplier.TestCase(
                                data,
                                "LiteralsEvaluator[lit=null]",
                                entirelyNullPreservesType == false && oc.getData().size() == 1 ? DataTypes.NULL : oc.expectedType,
                                nullValue(),
                                null,
                                oc.getExpectedTypeError()
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
    protected static List<TestCaseSupplier> errorsForCasesWithoutExamples(List<TestCaseSupplier> testCaseSuppliers) {
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
             */
            .filter(types -> types.stream().filter(t -> t == DataTypes.NULL).count() <= 1)
            .map(types -> typeErrorSupplier(validPerPosition.size() != 1, validPerPosition, types))
            .forEach(suppliers::add);
        return suppliers;
    }

    /**
     * Adds test cases containing unsupported parameter types that immediately fail.
     */
    protected static List<TestCaseSupplier> failureForCasesWithoutExamples(List<TestCaseSupplier> testCaseSuppliers) {
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
            .map(types -> new TestCaseSupplier("type error for " + TestCaseSupplier.nameFromTypes(types), types, () -> {
                throw new IllegalStateException("must implement a case for " + types);
            }))
            .forEach(suppliers::add);
        return suppliers;
    }

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
        if (argumentCount > 4) {
            // TODO check for a limit 4. is arbitrary.
            throw new IllegalArgumentException("would generate too many types");
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

    /**
     * Build a test case that asserts that the combination of parameter types is an error.
     */
    protected static TestCaseSupplier typeErrorSupplier(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types
    ) {
        return new TestCaseSupplier(
            "type error for " + TestCaseSupplier.nameFromTypes(types),
            types,
            () -> TestCaseSupplier.TestCase.typeError(
                types.stream().map(type -> new TestCaseSupplier.TypedData(randomLiteral(type).value(), type, type.typeName())).toList(),
                typeErrorMessage(includeOrdinal, validPerPosition, types)
            )
        );
    }

    /**
     * Build the expected error message for an invalid type signature.
     */
    private static String typeErrorMessage(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        int badArgPosition = -1;
        for (int i = 0; i < types.size(); i++) {
            if (validPerPosition.get(i).contains(types.get(i)) == false) {
                badArgPosition = i;
                break;
            }
        }
        if (badArgPosition == -1) {
            throw new IllegalStateException("can't find badArgPosition");
        }
        String ordinal = includeOrdinal ? TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " " : "";
        String expectedType = expectedType(validPerPosition.get(badArgPosition));
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [] must be [" + expectedType + "], found value [" + name + "] type [" + name + "]";
    }

    private static final Map<Set<DataType>, String> NAMED_EXPECTED_TYPES = Map.ofEntries(
        Map.entry(Set.of(DataTypes.DOUBLE, DataTypes.NULL), "double"),
        Map.entry(Set.of(DataTypes.INTEGER, DataTypes.NULL), "integer"),
        Map.entry(Set.of(DataTypes.LONG, DataTypes.INTEGER, DataTypes.UNSIGNED_LONG, DataTypes.DOUBLE, DataTypes.NULL), "numeric"),
        Map.entry(Set.of(DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.VERSION, DataTypes.NULL), "keyword, text or version"),
        Map.entry(Set.of(DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.NULL), "string"),
        Map.entry(Set.of(DataTypes.IP, DataTypes.KEYWORD, DataTypes.NULL), "ip or keyword")
    );

    private static String expectedType(Set<DataType> validTypes) {
        String named = NAMED_EXPECTED_TYPES.get(validTypes);
        if (named == null) {
            /*
             * Note for anyone who's test lands here - it's likely that you
             * don't have a test case covering explicit `null` arguments in
             * this position. Generally you can get that with anyNullIsNull.
             */
            throw new UnsupportedOperationException(
                "can't guess expected types for " + validTypes.stream().sorted(Comparator.comparing(t -> t.typeName())).toList()
            );
        }
        return named;
    }

    private static Stream<DataType> representable() {
        return EsqlDataTypes.types().stream().filter(EsqlDataTypes::isRepresentable);
    }

    @AfterClass
    public static void renderSignature() throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        if (ranAllTests == false) {
            LogManager.getLogger(getTestClass()).info("Skipping rendering signature because we're running a portion of the tests");
            return;
        }
        FunctionDefinition definition = definition();
        if (definition == null) {
            LogManager.getLogger(getTestClass()).info("Skipping rendering signature because the function isn't registered");
            return;
        }

        String rendered = RailRoadDiagram.functionSignature(definition);
        LogManager.getLogger(getTestClass()).info("Writing function signature");
        writeToTempDir("signature", rendered, "svg");
    }

    /**
     * Unique signatures encountered by this test.
     * <p>
     *     We clear this at the beginning of the test class with
     *     {@link #clearSignatures} out of paranoia. It <strong>is</strong>
     *     shared by many tests, after all.
     * </p>
     * <p>
     *     After each test method we add the signature it operated on via
     *     {@link #trackSignature}. Once the test class is done we render
     *     all the unique signatures to a temp file with {@link #renderTypesTable}.
     *     We use a temp file because that's all we're allowed to write to.
     *     Gradle will move the files into the docs after this is done.
     * </p>
     */
    private static final Map<List<DataType>, DataType> signatures = new HashMap<>();

    @BeforeClass
    public static void clearSignatures() {
        signatures.clear();
    }

    @After
    public void trackSignature() {
        if (testCase.getExpectedTypeError() != null) {
            return;
        }
        if (testCase.getData().stream().anyMatch(t -> t.type() == DataTypes.NULL)) {
            return;
        }
        signatures.putIfAbsent(testCase.getData().stream().map(TestCaseSupplier.TypedData::type).toList(), testCase.expectedType);
    }

    @AfterClass
    public static void renderTypesTable() throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        FunctionDefinition definition = definition();
        if (definition == null) {
            LogManager.getLogger(getTestClass()).info("Skipping rendering types because the function isn't registered");
            return;
        }

        List<String> args = EsqlFunctionRegistry.description(definition).argNames();
        StringBuilder header = new StringBuilder();
        for (String arg : args) {
            header.append(arg).append(" | ");
        }
        header.append("result");

        List<String> table = new ArrayList<>();
        for (Map.Entry<List<DataType>, DataType> sig : signatures.entrySet()) {
            if (sig.getKey().size() != args.size()) {
                continue;
            }
            StringBuilder b = new StringBuilder();
            for (DataType arg : sig.getKey()) {
                b.append(arg.typeName()).append(" | ");
            }
            b.append(sig.getValue().typeName());
            table.add(b.toString());
        }
        Collections.sort(table);

        String rendered = """
            [%header.monospaced.styled,format=dsv,separator=|]
            |===
            """ + header + "\n" + table.stream().collect(Collectors.joining("\n")) + "\n|===\n";
        LogManager.getLogger(getTestClass()).info("Writing function types:\n{}", rendered);
        writeToTempDir("types", rendered, "asciidoc");
    }

    private static FunctionDefinition definition() {
        String name = functionName();
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        if (registry.functionExists(name)) {
            return registry.resolveFunction(name);
        }
        return null;
    }

    private static String functionName() {
        return StringUtils.camelCaseToUnderscore(getTestClass().getSimpleName().replace("Tests", "")).toLowerCase(Locale.ROOT);
    }

    /**
     * Write some text to a tempdir so we can copy it to the docs later.
     * <p>
     *     We need to write to a tempdir instead of the docs because the tests
     *     don't have write permission to the docs.
     * </p>
     */
    private static void writeToTempDir(String subdir, String str, String extension) throws IOException {
        // We have to write to a tempdir because it's all test are allowed to write to. Gradle can move them.
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("functions").resolve(subdir);
        Files.createDirectories(dir);
        Path file = dir.resolve(functionName() + "." + extension);
        Files.writeString(file, str);
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    protected final DriverContext driverContext() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays.withCircuitBreaking(), new BlockFactory(breaker, bigArrays));
    }

    protected final DriverContext crankyContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new CrankyCircuitBreakerService());
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays.withCircuitBreaking(), new BlockFactory(breaker, bigArrays));
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    static Version randomVersion() {
        // TODO degenerate versions and stuff
        return switch (between(0, 2)) {
            case 0 -> new Version(Integer.toString(between(0, 100)));
            case 1 -> new Version(between(0, 100) + "." + between(0, 100));
            case 2 -> new Version(between(0, 100) + "." + between(0, 100) + "." + between(0, 100));
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * All string types (keyword, text, match_only_text, etc).
     */
    protected static DataType[] strings() {
        return EsqlDataTypes.types().stream().filter(DataTypes::isString).toArray(DataType[]::new);
    }
}
