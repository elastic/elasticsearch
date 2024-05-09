/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import com.carrotsearch.randomizedtesting.ClassModel;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.NumericUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isSpatial;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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
            case "integer", "counter_integer" -> randomInt();
            case "unsigned_long", "long", "counter_long" -> randomLong();
            case "date_period" -> Period.of(randomIntBetween(-1000, 1000), randomIntBetween(-13, 13), randomIntBetween(-32, 32));
            case "datetime" -> randomMillisUpToYear9999();
            case "double", "scaled_float", "counter_double" -> randomDouble();
            case "float" -> randomFloat();
            case "half_float" -> HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
            case "keyword" -> new BytesRef(randomAlphaOfLength(5));
            case "ip" -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
            case "time_duration" -> Duration.ofMillis(randomLongBetween(-604800000L, 604800000L)); // plus/minus 7 days
            case "text" -> new BytesRef(randomAlphaOfLength(50));
            case "version" -> randomVersion().toBytesRef();
            case "geo_point" -> GEO.asWkb(GeometryTestUtils.randomPoint());
            case "cartesian_point" -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint());
            case "geo_shape" -> GEO.asWkb(GeometryTestUtils.randomGeometry(randomBoolean()));
            case "cartesian_shape" -> CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(randomBoolean()));
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

    /**
     * Build an {@link Attribute} that loads a field.
     */
    public static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true));
    }

    /**
     * Build an {@link Attribute} that loads a field and then creates a deep copy of its data.
     */
    public static Expression deepCopyOfField(String name, DataType type) {
        return new DeepCopy(Source.EMPTY, new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true)));
    }

    /**
     * Build the expression being tested, for the given source and list of arguments.  Test classes need to implement this
     * to have something to test.
     *
     * @param source the source
     * @param args   arg list from the test case, should match the length expected
     * @return an expression for evaluating the function being tested on the given arguments
     */
    protected abstract Expression build(Source source, List<Expression> args);

    /**
     * Build an {@link Expression} where all inputs are field references,
     * <strong>except</strong> those that have been marked with {@link TestCaseSupplier.TypedData#forceLiteral()}.
     */
    protected final Expression buildFieldExpression(TestCaseSupplier.TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsFields());
    }

    /**
     * Build an {@link Expression} where all inputs are anonymous functions
     * that make a copy of the values from a field <strong>except</strong>
     * those that have been marked with {@link TestCaseSupplier.TypedData#forceLiteral()}.
     */
    protected final Expression buildDeepCopyOfFieldExpression(TestCaseSupplier.TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsDeepCopiedFields());
    }

    protected final Expression buildLiteralExpression(TestCaseSupplier.TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsLiterals());
    }

    /**
     * Convert an {@link Expression} tree into a {@link ExpressionEvaluator.Factory}
     * for {@link ExpressionEvaluator}s in the same way as our planner.
     */
    public static ExpressionEvaluator.Factory evaluator(Expression e) {
        e = new FoldNull().rule(e);
        if (e.foldable()) {
            e = new Literal(e.source(), e.fold(), e.dataType());
        }
        Layout.Builder builder = new Layout.Builder();
        buildLayout(builder, e);
        Expression.TypeResolution resolution = e.typeResolved();
        if (resolution.unresolved()) {
            throw new AssertionError("expected resolved " + resolution.message());
        }
        return EvalMapper.toEvaluator(e, builder.build());
    }

    protected final Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    /**
     * Hack together a layout by scanning for Fields.
     * Those will show up in the layout in whatever order a depth first traversal finds them.
     */
    protected static void buildLayout(Layout.Builder builder, Expression e) {
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
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", EsqlDataTypes.isRepresentable(testCase.expectedType()));
        logger.info(
            "Test Values: " + testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","))
        );
        boolean readFloating = randomBoolean();
        Expression expression = readFloating ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTrue("expected unresolved", expression.typeResolved().unresolved());
            if (readFloating == false) {
                // The hack that creates floating fields changes the error message so don't assert it
                assertThat(expression.typeResolved().message(), equalTo(testCase.getExpectedTypeError()));
            }
            return;
        }
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

    private Object toJavaObjectUnsignedLongAware(Block block, int position) {
        Object result;
        result = toJavaObject(block, position);
        if (result != null && testCase.expectedType() == DataTypes.UNSIGNED_LONG) {
            assertThat(result, instanceOf(Long.class));
            result = NumericUtils.unsignedLongAsBigInteger((Long) result);
        }
        return result;
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
            assertFalse("Test data is too large to fit in the memory", true);
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

    /**
     * Does the function produce the same output regardless of input?
     */
    protected Matcher<Object> allNullsMatcher() {
        return nullValue();
    }

    private void testEvaluateBlock(BlockFactory inputBlockFactory, DriverContext context, boolean insertNulls) {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", EsqlDataTypes.isRepresentable(testCase.expectedType()));
        assumeTrue("Must build evaluator to test sending it blocks", testCase.getExpectedTypeError() == null);
        boolean readFloating = randomBoolean();
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
            Expression expression = readFloating ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
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

    // TODO cranky time

    public void testSimpleWithNulls() { // TODO replace this with nulls inserted into the test case like anyNullIsNull
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Nothing to do if a type error", testCase.getExpectedTypeError() == null);
        List<Object> simpleData = testCase.getDataValues();
        try (EvalOperator.ExpressionEvaluator eval = evaluator(buildFieldExpression(testCase)).get(driverContext())) {
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

    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        // TODO remove me in favor of cases containing null
        assertTrue("argument " + nullBlock + " is null", value.isNull(0));
    }

    public final void testEvaluateInManyThreads() throws ExecutionException, InterruptedException {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Expected type must be representable to build an evaluator", EsqlDataTypes.isRepresentable(testCase.expectedType()));
        assumeTrue("Nothing to do if a type error", testCase.getExpectedTypeError() == null);
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
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Nothing to do if a type error", testCase.getExpectedTypeError() == null);
        var factory = evaluator(buildFieldExpression(testCase));
        try (ExpressionEvaluator ev = factory.get(driverContext())) {
            assertThat(ev.toString(), testCase.evaluatorToString());
        }
    }

    public final void testFactoryToString() {
        assumeTrue("Can't build evaluator", testCase.canBuildEvaluator());
        assumeTrue("Nothing to do if a type error", testCase.getExpectedTypeError() == null);
        var factory = evaluator(buildFieldExpression(testCase));
        assertThat(factory.toString(), testCase.evaluatorToString());
    }

    public final void testFold() {
        Expression expression = buildLiteralExpression(testCase);
        if (testCase.getExpectedTypeError() != null) {
            assertTrue(expression.typeResolved().unresolved());
            assertThat(expression.typeResolved().message(), equalTo(testCase.getExpectedTypeError()));
            return;
        }
        assertFalse(expression.typeResolved().unresolved());
        Expression nullOptimized = new FoldNull().rule(expression);
        assertThat(nullOptimized.dataType(), equalTo(testCase.expectedType()));
        assertTrue(nullOptimized.foldable());
        if (testCase.foldingExceptionClass() == null) {
            Object result = nullOptimized.fold();
            // Decode unsigned longs into BigIntegers
            if (testCase.expectedType() == DataTypes.UNSIGNED_LONG && result != null) {
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

    public void testSerializationOfSimple() {
        assertSerialization(buildFieldExpression(testCase));
    }

    @AfterClass
    public static void testFunctionInfo() {
        Logger log = LogManager.getLogger(getTestClass());
        FunctionDefinition definition = definition(functionName());
        if (definition == null) {
            log.info("Skipping function info checks because the function isn't registered");
            return;
        }
        // TODO fix case tests to include all supported types
        assumeFalse("CASE test incomplete", definition.name().equals("case"));
        log.info("Running function info checks");
        EsqlFunctionRegistry.FunctionDescription description = EsqlFunctionRegistry.description(definition);
        List<EsqlFunctionRegistry.ArgSignature> args = description.args();

        assertTrue("expect description to be defined", description.description() != null && false == description.description().isEmpty());

        List<Set<String>> typesFromSignature = new ArrayList<>();
        Set<String> returnFromSignature = new HashSet<>();
        for (int i = 0; i < args.size(); i++) {
            typesFromSignature.add(new HashSet<>());
        }
        Function<DataType, String> typeName = dt -> dt.esType() != null ? dt.esType() : dt.typeName();
        for (Map.Entry<List<DataType>, DataType> entry : signatures().entrySet()) {
            List<DataType> types = entry.getKey();
            for (int i = 0; i < args.size() && i < types.size(); i++) {
                typesFromSignature.get(i).add(typeName.apply(types.get(i)));
            }
            returnFromSignature.add(typeName.apply(entry.getValue()));
        }

        for (int i = 0; i < args.size(); i++) {
            EsqlFunctionRegistry.ArgSignature arg = args.get(i);
            Set<String> annotationTypes = Arrays.stream(arg.type()).collect(Collectors.toCollection(TreeSet::new));
            Set<String> signatureTypes = typesFromSignature.get(i);
            if (signatureTypes.isEmpty()) {
                log.info("{}: skipping", arg.name());
                continue;
            }
            log.info("{}: tested {} vs annotated {}", arg.name(), signatureTypes, annotationTypes);
            assertEquals(signatureTypes, annotationTypes);
        }

        Set<String> returnTypes = Arrays.stream(description.returnType()).collect(Collectors.toCollection(TreeSet::new));
        assertEquals(returnFromSignature, returnTypes);

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
                && nullValueDataType == DataTypes.NULL
                && original.getData().size() == 1 ? DataTypes.NULL : original.expectedType(),
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
                        return i == finalNullPosition ? od.forceValueToNull() : od;
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
                        .mapToObj(i -> i == finalNullPosition ? DataTypes.NULL : original.types().get(i))
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
                                expectedType.expectedType(finalNullPosition, DataTypes.NULL, oc),
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
    protected static List<TestCaseSupplier> errorsForCasesWithoutExamples(List<TestCaseSupplier> testCaseSuppliers) {
        return errorsForCasesWithoutExamples(testCaseSuppliers, AbstractFunctionTestCase::typeErrorMessage);
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
             */.filter(types -> types.stream().filter(t -> t == DataTypes.NULL).count() <= 1)
            .map(types -> typeErrorSupplier(validPerPosition.size() != 1, validPerPosition, types, typeErrorMessageSupplier))
            .forEach(suppliers::add);
        return suppliers;
    }

    public static String errorMessageStringForBinaryOperators(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types
    ) {
        try {
            return typeErrorMessage(includeOrdinal, validPerPosition, types);
        } catch (IllegalStateException e) {
            // This means all the positional args were okay, so the expected error is from the combination
            if (types.get(0).equals(DataTypes.UNSIGNED_LONG)) {
                return "first argument of [] is [unsigned_long] and second is ["
                    + types.get(1).typeName()
                    + "]. [unsigned_long] can only be operated on together with another [unsigned_long]";

            }
            if (types.get(1).equals(DataTypes.UNSIGNED_LONG)) {
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

    protected static TestCaseSupplier typeErrorSupplier(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types
    ) {
        return typeErrorSupplier(includeOrdinal, validPerPosition, types, AbstractFunctionTestCase::typeErrorMessage);
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
    protected static String typeErrorMessage(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
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
        String expectedType = expectedType(validPerPosition.get(badArgPosition));
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [] must be [" + expectedType + "], found value [" + name + "] type [" + name + "]";
    }

    private static final Map<Set<DataType>, String> NAMED_EXPECTED_TYPES = Map.ofEntries(
        Map.entry(
            Set.of(
                EsqlDataTypes.DATE_PERIOD,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.LONG,
                EsqlDataTypes.TIME_DURATION,
                DataTypes.NULL
            ),
            "numeric, date_period or time_duration"
        ),
        Map.entry(Set.of(DataTypes.DATETIME, DataTypes.NULL), "datetime"),
        Map.entry(Set.of(DataTypes.DOUBLE, DataTypes.NULL), "double"),
        Map.entry(Set.of(DataTypes.INTEGER, DataTypes.NULL), "integer"),
        Map.entry(Set.of(DataTypes.IP, DataTypes.NULL), "ip"),
        Map.entry(Set.of(DataTypes.LONG, DataTypes.INTEGER, DataTypes.UNSIGNED_LONG, DataTypes.DOUBLE, DataTypes.NULL), "numeric"),
        Map.entry(Set.of(DataTypes.LONG, DataTypes.INTEGER, DataTypes.UNSIGNED_LONG, DataTypes.DOUBLE), "numeric"),
        Map.entry(Set.of(DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.VERSION, DataTypes.NULL), "string or version"),
        Map.entry(Set.of(DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.NULL), "string"),
        Map.entry(Set.of(DataTypes.IP, DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.NULL), "ip or string"),
        Map.entry(Set.copyOf(Arrays.asList(representableTypes())), "representable"),
        Map.entry(Set.copyOf(Arrays.asList(representableNonSpatialTypes())), "representableNonSpatial"),
        Map.entry(
            Set.of(
                DataTypes.BOOLEAN,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.NULL
            ),
            "boolean or numeric or string"
        ),
        Map.entry(
            Set.of(
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.NULL
            ),
            "datetime or numeric or string"
        ),
        // What Add accepts
        Map.entry(
            Set.of(
                EsqlDataTypes.DATE_PERIOD,
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.LONG,
                DataTypes.NULL,
                EsqlDataTypes.TIME_DURATION,
                DataTypes.UNSIGNED_LONG
            ),
            "datetime or numeric"
        ),
        Map.entry(
            Set.of(
                DataTypes.BOOLEAN,
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.NULL
            ),
            "boolean or datetime or numeric or string"
        ),
        // to_int
        Map.entry(
            Set.of(
                DataTypes.BOOLEAN,
                EsqlDataTypes.COUNTER_INTEGER,
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.NULL
            ),
            "boolean or counter_integer or datetime or numeric or string"
        ),
        // to_long
        Map.entry(
            Set.of(
                DataTypes.BOOLEAN,
                EsqlDataTypes.COUNTER_INTEGER,
                EsqlDataTypes.COUNTER_LONG,
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.NULL
            ),
            "boolean or counter_integer or counter_long or datetime or numeric or string"
        ),
        // to_double
        Map.entry(
            Set.of(
                DataTypes.BOOLEAN,
                EsqlDataTypes.COUNTER_DOUBLE,
                EsqlDataTypes.COUNTER_INTEGER,
                EsqlDataTypes.COUNTER_LONG,
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.NULL
            ),
            "boolean or counter_double or counter_integer or counter_long or datetime or numeric or string"
        ),
        Map.entry(
            Set.of(
                DataTypes.BOOLEAN,
                EsqlDataTypes.CARTESIAN_POINT,
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                EsqlDataTypes.GEO_POINT,
                DataTypes.INTEGER,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.NULL
            ),
            "boolean or cartesian_point or datetime or geo_point or numeric or string"
        ),
        Map.entry(
            Set.of(
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                DataTypes.INTEGER,
                DataTypes.IP,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.VERSION,
                DataTypes.NULL
            ),
            "datetime, double, integer, ip, keyword, long, text, unsigned_long or version"
        ),
        Map.entry(
            Set.of(
                DataTypes.BOOLEAN,
                DataTypes.DATETIME,
                DataTypes.DOUBLE,
                EsqlDataTypes.GEO_POINT,
                EsqlDataTypes.GEO_SHAPE,
                DataTypes.INTEGER,
                DataTypes.IP,
                DataTypes.KEYWORD,
                DataTypes.LONG,
                DataTypes.TEXT,
                DataTypes.UNSIGNED_LONG,
                DataTypes.VERSION,
                DataTypes.NULL
            ),
            "cartesian_point or datetime or geo_point or numeric or string"
        ),
        Map.entry(Set.of(EsqlDataTypes.GEO_POINT, DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.NULL), "geo_point or string"),
        Map.entry(Set.of(EsqlDataTypes.CARTESIAN_POINT, DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.NULL), "cartesian_point or string"),
        Map.entry(
            Set.of(EsqlDataTypes.GEO_POINT, EsqlDataTypes.GEO_SHAPE, DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.NULL),
            "geo_point or geo_shape or string"
        ),
        Map.entry(
            Set.of(EsqlDataTypes.CARTESIAN_POINT, EsqlDataTypes.CARTESIAN_SHAPE, DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.NULL),
            "cartesian_point or cartesian_shape or string"
        ),
        Map.entry(Set.of(EsqlDataTypes.GEO_POINT, EsqlDataTypes.CARTESIAN_POINT, DataTypes.NULL), "geo_point or cartesian_point"),
        Map.entry(Set.of(EsqlDataTypes.DATE_PERIOD, EsqlDataTypes.TIME_DURATION, DataTypes.NULL), "dateperiod or timeduration")
    );

    // TODO: generate this message dynamically, a la AbstractConvertFunction#supportedTypesNames()?
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

    protected static Stream<DataType> representable() {
        return EsqlDataTypes.types().stream().filter(EsqlDataTypes::isRepresentable);
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

    @AfterClass
    public static void renderSignature() throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        String rendered = buildSignatureSvg(functionName());
        if (rendered == null) {
            LogManager.getLogger(getTestClass()).info("Skipping rendering signature because the function isn't registered");
        } else {
            LogManager.getLogger(getTestClass()).info("Writing function signature");
            writeToTempDir("signature", rendered, "svg");
        }
    }

    private static String buildSignatureSvg(String name) throws IOException {
        String binaryOperator = binaryOperator(name);
        if (binaryOperator != null) {
            return RailRoadDiagram.binaryOperator(binaryOperator);
        }
        String unaryOperator = unaryOperator(name);
        if (unaryOperator != null) {
            return RailRoadDiagram.unaryOperator(unaryOperator);
        }
        FunctionDefinition definition = definition(name);
        if (definition != null) {
            return RailRoadDiagram.functionSignature(definition);
        }
        return null;
    }

    private static Class<?> classGeneratingSignatures = null;
    /**
     * Unique signatures in this test's parameters.
     */
    private static Map<List<DataType>, DataType> signatures;

    private static Map<List<DataType>, DataType> signatures() {
        Class<?> testClass = getTestClass();
        if (signatures != null && classGeneratingSignatures == testClass) {
            return signatures;
        }
        classGeneratingSignatures = testClass;
        signatures = new HashMap<>();
        Set<Method> paramsFactories = new ClassModel(testClass).getAnnotatedLeafMethods(ParametersFactory.class).keySet();
        assertThat(paramsFactories, hasSize(1));
        Method paramsFactory = paramsFactories.iterator().next();
        List<?> params;
        try {
            params = (List<?>) paramsFactory.invoke(null);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        for (Object p : params) {
            TestCaseSupplier tcs = (TestCaseSupplier) ((Object[]) p)[0];
            TestCaseSupplier.TestCase tc = tcs.get();
            if (tc.getExpectedTypeError() != null) {
                continue;
            }
            if (tc.getData().stream().anyMatch(t -> t.type() == DataTypes.NULL)) {
                continue;
            }
            signatures.putIfAbsent(tc.getData().stream().map(TestCaseSupplier.TypedData::type).toList(), tc.expectedType());
        }
        return signatures;
    }

    @AfterClass
    public static void renderDocs() throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        String name = functionName();
        if (binaryOperator(name) != null) {
            renderTypes(List.of("lhs", "rhs"));
            return;
        }
        if (unaryOperator(name) != null) {
            renderTypes(List.of("v"));
            return;
        }
        if (name.equalsIgnoreCase("rlike")) {
            renderTypes(List.of("str", "pattern", "caseInsensitive"));
            return;
        }
        if (name.equalsIgnoreCase("like")) {
            renderTypes(List.of("str", "pattern"));
            return;
        }
        FunctionDefinition definition = definition(name);
        if (definition != null) {
            EsqlFunctionRegistry.FunctionDescription description = EsqlFunctionRegistry.description(definition);
            renderTypes(description.argNames());
            renderParametersList(description.argNames(), description.argDescriptions());
            FunctionInfo info = EsqlFunctionRegistry.functionInfo(definition);
            renderDescription(description.description(), info.detailedDescription(), info.note());
            boolean hasExamples = renderExamples(info);
            renderFullLayout(name, hasExamples);
            renderKibanaInlineDocs(name, info);
            List<EsqlFunctionRegistry.ArgSignature> args = description.args();
            if (name.equals("case")) {
                EsqlFunctionRegistry.ArgSignature falseValue = args.get(1);
                args = List.of(
                    args.get(0),
                    falseValue,
                    new EsqlFunctionRegistry.ArgSignature(
                        "falseValue",
                        falseValue.type(),
                        falseValue.description(),
                        true,
                        EsqlFunctionRegistry.getTargetType(falseValue.type())
                    )
                );
            }
            renderKibanaFunctionDefinition(name, info, args, description.variadic());
            return;
        }
        LogManager.getLogger(getTestClass()).info("Skipping rendering types because the function '" + name + "' isn't registered");
    }

    private static final String DOCS_WARNING =
        "// This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.\n\n";

    private static void renderTypes(List<String> argNames) throws IOException {
        StringBuilder header = new StringBuilder();
        for (String arg : argNames) {
            header.append(arg).append(" | ");
        }
        header.append("result");

        List<String> table = new ArrayList<>();
        for (Map.Entry<List<DataType>, DataType> sig : signatures().entrySet()) { // TODO flip to using sortedSignatures
            if (sig.getKey().size() > argNames.size()) { // skip variadic [test] cases (but not those with optional parameters)
                continue;
            }
            StringBuilder b = new StringBuilder();
            for (DataType arg : sig.getKey()) {
                b.append(arg.typeName()).append(" | ");
            }
            b.append("| ".repeat(argNames.size() - sig.getKey().size()));
            b.append(sig.getValue().typeName());
            table.add(b.toString());
        }
        Collections.sort(table);
        if (table.isEmpty()) {
            table.add(signatures.values().iterator().next().typeName());
        }

        String rendered = DOCS_WARNING + """
            *Supported types*

            [%header.monospaced.styled,format=dsv,separator=|]
            |===
            """ + header + "\n" + table.stream().collect(Collectors.joining("\n")) + "\n|===\n";
        LogManager.getLogger(getTestClass()).info("Writing function types for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("types", rendered, "asciidoc");
    }

    private static void renderParametersList(List<String> argNames, List<String> argDescriptions) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append(DOCS_WARNING);
        builder.append("*Parameters*\n");
        for (int a = 0; a < argNames.size(); a++) {
            builder.append("\n`").append(argNames.get(a)).append("`::\n").append(argDescriptions.get(a)).append('\n');
        }
        String rendered = builder.toString();
        LogManager.getLogger(getTestClass()).info("Writing parameters for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("parameters", rendered, "asciidoc");
    }

    private static void renderDescription(String description, String detailedDescription, String note) throws IOException {
        String rendered = DOCS_WARNING + """
            *Description*

            """ + description + "\n";

        if (Strings.isNullOrEmpty(detailedDescription) == false) {
            rendered += "\n" + detailedDescription + "\n";
        }

        if (Strings.isNullOrEmpty(note) == false) {
            rendered += "\nNOTE: " + note + "\n";
        }
        LogManager.getLogger(getTestClass()).info("Writing description for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("description", rendered, "asciidoc");
    }

    private static boolean renderExamples(FunctionInfo info) throws IOException {
        if (info == null || info.examples().length == 0) {
            return false;
        }
        StringBuilder builder = new StringBuilder();
        builder.append(DOCS_WARNING);
        if (info.examples().length == 1) {
            builder.append("*Example*\n\n");
        } else {
            builder.append("*Examples*\n\n");
        }
        for (Example example : info.examples()) {
            if (example.description().length() > 0) {
                builder.append(example.description());
                builder.append("\n");
            }
            builder.append("""
                [source.merge.styled,esql]
                ----
                include::{esql-specs}/$FILE$.csv-spec[tag=$TAG$]
                ----
                [%header.monospaced.styled,format=dsv,separator=|]
                |===
                include::{esql-specs}/$FILE$.csv-spec[tag=$TAG$-result]
                |===
                """.replace("$FILE$", example.file()).replace("$TAG$", example.tag()));
            if (example.explanation().length() > 0) {
                builder.append("\n");
                builder.append(example.explanation());
                builder.append("\n\n");
            }
        }
        builder.append('\n');
        String rendered = builder.toString();
        LogManager.getLogger(getTestClass()).info("Writing examples for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("examples", rendered, "asciidoc");
        return true;
    }

    private static void renderFullLayout(String name, boolean hasExamples) throws IOException {
        String rendered = DOCS_WARNING + """
            [discrete]
            [[esql-$NAME$]]
            === `$UPPER_NAME$`

            *Syntax*

            [.text-center]
            image::esql/functions/signature/$NAME$.svg[Embedded,opts=inline]

            include::../parameters/$NAME$.asciidoc[]
            include::../description/$NAME$.asciidoc[]
            include::../types/$NAME$.asciidoc[]
            """.replace("$NAME$", name).replace("$UPPER_NAME$", name.toUpperCase(Locale.ROOT));
        if (hasExamples) {
            rendered += "include::../examples/" + name + ".asciidoc[]\n";
        }
        LogManager.getLogger(getTestClass()).info("Writing layout for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("layout", rendered, "asciidoc");
    }

    private static void renderKibanaInlineDocs(String name, FunctionInfo info) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("""
            <!--
            This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.
            -->

            """);
        builder.append("### ").append(name.toUpperCase(Locale.ROOT)).append("\n");
        builder.append(removeAsciidocLinks(info.description())).append("\n\n");

        if (info.examples().length > 0) {
            Example example = info.examples()[0];
            builder.append("```\n");
            builder.append("read-example::").append(example.file()).append(".csv-spec[tag=").append(example.tag()).append("]\n");
            builder.append("```\n");
        }
        if (Strings.isNullOrEmpty(info.note()) == false) {
            builder.append("Note: ").append(removeAsciidocLinks(info.note())).append("\n");
        }
        String rendered = builder.toString();
        LogManager.getLogger(getTestClass()).info("Writing kibana inline docs for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("kibana/docs", rendered, "md");
    }

    private static void renderKibanaFunctionDefinition(
        String name,
        FunctionInfo info,
        List<EsqlFunctionRegistry.ArgSignature> args,
        boolean variadic
    ) throws IOException {

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().lfAtEnd().startObject();
        builder.field(
            "comment",
            "This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it."
        );
        builder.field("type", "eval"); // TODO aggs in here too
        builder.field("name", name);
        builder.field("description", removeAsciidocLinks(info.description()));
        if (Strings.isNullOrEmpty(info.note()) == false) {
            builder.field("note", removeAsciidocLinks(info.note()));
        }
        // TODO aliases

        builder.startArray("signatures");
        if (args.isEmpty()) {
            builder.startObject();
            builder.startArray("params");
            builder.endArray();
            // There should only be one return type so just use that as the example
            builder.field("returnType", signatures().values().iterator().next().typeName());
            builder.endObject();
        } else {
            int minArgCount = (int) args.stream().filter(a -> false == a.optional()).count();
            for (Map.Entry<List<DataType>, DataType> sig : sortedSignatures()) {
                if (variadic && sig.getKey().size() > args.size()) {
                    // For variadic functions we test much longer signatures, let's just stop at the last one
                    continue;
                }
                if (sig.getKey().size() < minArgCount) {
                    throw new IllegalArgumentException("signature " + sig.getKey() + " is missing non-optional arg for " + args);
                }
                builder.startObject();
                builder.startArray("params");
                for (int i = 0; i < sig.getKey().size(); i++) {
                    EsqlFunctionRegistry.ArgSignature arg = args.get(i);
                    builder.startObject();
                    builder.field("name", arg.name());
                    builder.field("type", sig.getKey().get(i).typeName());
                    builder.field("optional", arg.optional());
                    builder.field("description", arg.description());
                    builder.endObject();
                }
                builder.endArray();
                builder.field("variadic", variadic);
                builder.field("returnType", sig.getValue().typeName());
                builder.endObject();
            }
        }
        builder.endArray();

        if (info.examples().length > 0) {
            builder.startArray("examples");
            for (Example example : info.examples()) {
                builder.value("read-example::" + example.file() + ".csv-spec[tag=" + example.tag() + ", json]");
            }
            builder.endArray();
        }

        String rendered = Strings.toString(builder.endObject());
        LogManager.getLogger(getTestClass()).info("Writing kibana function definition for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("kibana/definition", rendered, "json");
    }

    private static String removeAsciidocLinks(String asciidoc) {
        return asciidoc.replaceAll("[^ ]+\\[([^\\]]+)\\]", "$1");
    }

    private static List<Map.Entry<List<DataType>, DataType>> sortedSignatures() {
        List<Map.Entry<List<DataType>, DataType>> sortedSignatures = new ArrayList<>(signatures().entrySet());
        Collections.sort(sortedSignatures, new Comparator<>() {
            @Override
            public int compare(Map.Entry<List<DataType>, DataType> lhs, Map.Entry<List<DataType>, DataType> rhs) {
                int maxlen = Math.max(lhs.getKey().size(), rhs.getKey().size());
                for (int i = 0; i < maxlen; i++) {
                    if (lhs.getKey().size() <= i) {
                        return -1;
                    }
                    if (rhs.getKey().size() <= i) {
                        return 1;
                    }
                    int c = lhs.getKey().get(i).typeName().compareTo(rhs.getKey().get(i).typeName());
                    if (c != 0) {
                        return c;
                    }
                }
                return lhs.getValue().typeName().compareTo(rhs.getValue().typeName());
            }
        });
        return sortedSignatures;
    }

    protected static String functionName() {
        Class<?> testClass = getTestClass();
        if (testClass.isAnnotationPresent(FunctionName.class)) {
            FunctionName functionNameAnnotation = testClass.getAnnotation(FunctionName.class);
            return functionNameAnnotation.value();
        } else {
            return StringUtils.camelCaseToUnderscore(testClass.getSimpleName().replace("Tests", "")).toLowerCase(Locale.ROOT);
        }
    }

    private static FunctionDefinition definition(String name) {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        if (registry.functionExists(name)) {
            return registry.resolveFunction(name);
        }
        return null;
    }

    /**
     * If this test is a for a binary operator return its symbol, otherwise return {@code null}.
     * This is functionally the reverse of the combination of
     * {@link ExpressionBuilder#visitArithmeticBinary} and {@link ExpressionBuilder#visitComparison}.
     */
    private static String binaryOperator(String name) {
        return switch (name) {
            case "add" -> "+";
            case "div" -> "/";
            case "equals" -> "==";
            case "greater_than" -> ">";
            case "greater_than_or_equal" -> ">=";
            case "less_than" -> "<";
            case "less_than_or_equal" -> "<=";
            case "mod" -> "%";
            case "mul" -> "*";
            case "not_equals" -> "!=";
            case "sub" -> "-";
            default -> null;
        };
    }

    /**
     * If this tests is for a unary operator return its symbol, otherwise return {@code null}.
     * This is functionally the reverse of {@link ExpressionBuilder#visitArithmeticUnary}.
     */
    private static String unaryOperator(String name) {
        return switch (name) {
            case "neg" -> "-";
            default -> null;
        };
    }

    /**
     * Write some text to a tempdir so we can copy it to the docs later.
     * <p>
     * We need to write to a tempdir instead of the docs because the tests
     * don't have write permission to the docs.
     * </p>
     */
    private static void writeToTempDir(String subdir, String str, String extension) throws IOException {
        // We have to write to a tempdir because it's all test are allowed to write to. Gradle can move them.
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("functions").resolve(subdir);
        Files.createDirectories(dir);
        Path file = dir.resolve(functionName() + "." + extension);
        Files.writeString(file, str);
        LogManager.getLogger(getTestClass()).info("Wrote to file: {}", file);
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    protected final DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
    }

    protected final DriverContext crankyContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new CrankyCircuitBreakerService())
            .withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
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
