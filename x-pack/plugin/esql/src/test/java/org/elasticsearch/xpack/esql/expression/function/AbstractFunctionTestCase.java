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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowFunctions;
import org.elasticsearch.xpack.esql.planner.Layout;
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
import org.elasticsearch.xpack.ql.util.NumericUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base class for function tests.  Tests based on this class will generally build out a single example evaluation,
 * which can be automatically tested against several scenarios (null handling, concurrency, etc).
 */
public abstract class AbstractFunctionTestCase extends ESTestCase {

    /**
     * Holds a data value and the intended parse type of that value
     * @param data - value to test against
     * @param type - type of the value, for building expressions
     */
    public record TypedData(Object data, DataType type, String name) {
        public TypedData(Object data, String name) {
            this(data, EsqlDataTypes.fromJava(data), name);
        }
    }

    public static class TestCase {
        /**
         * The {@link Source} this test case should be run with
         */
        private Source source;
        /**
         * The parameter values and types to pass into the function for this test run
         */
        private List<TypedData> data;

        /**
         * The expected toString output for the evaluator this fuction invocation should generate
         */
        String evaluatorToString;
        /**
         * The expected output type for the case being tested
         */
        DataType expectedType;
        /**
         * A matcher to validate the output of the function run on the given input data
         */
        private Matcher<Object> matcher;

        /**
         * Warnings this test is expected to produce
         */
        private String[] expectedWarnings;

        private final String expectedTypeError;

        public TestCase(List<TypedData> data, String evaluatorToString, DataType expectedType, Matcher<Object> matcher) {
            this(data, evaluatorToString, expectedType, matcher, null, null);
        }

        public static TestCase typeError(List<TypedData> data, String expectedTypeError) {
            return new TestCase(data, null, null, null, null, expectedTypeError);
        }

        private TestCase(
            List<TypedData> data,
            String evaluatorToString,
            DataType expectedType,
            Matcher<Object> matcher,
            String[] expectedWarnings,
            String expectedTypeError
        ) {
            this.source = Source.EMPTY;
            this.data = data;
            this.evaluatorToString = evaluatorToString;
            this.expectedType = expectedType;
            this.matcher = matcher;
            this.expectedWarnings = expectedWarnings;
            this.expectedTypeError = expectedTypeError;
        }

        public Source getSource() {
            return source;
        }

        public List<TypedData> getData() {
            return data;
        }

        public List<Expression> getDataAsFields() {
            return data.stream().map(t -> field(t.name(), t.type())).collect(Collectors.toList());
        }

        public List<Expression> getDataAsLiterals() {
            return data.stream().map(t -> new Literal(Source.synthetic(t.name()), t.data(), t.type())).collect(Collectors.toList());
        }

        public List<Object> getDataValues() {
            return data.stream().map(t -> t.data()).collect(Collectors.toList());
        }

        public Matcher<Object> getMatcher() {
            return matcher;
        }

        public TestCase withWarning(String warning) {
            String[] newWarnings;
            if (expectedWarnings != null) {
                newWarnings = Arrays.copyOf(expectedWarnings, expectedWarnings.length + 1);
                newWarnings[expectedWarnings.length] = warning;
            } else {
                newWarnings = new String[] { warning };
            }
            return new TestCase(data, evaluatorToString, expectedType, matcher, newWarnings, expectedTypeError);
        }
    }

    /**
     * This class exists to give a human-readable string representation of the test case.
     */
    public record TestCaseSupplier(String name, List<DataType> types, Supplier<TestCase> supplier) implements Supplier<TestCase> {
        /**
         * Build a test case without types.
         * @deprecated Supply types
         */
        @Deprecated
        public TestCaseSupplier(String name, Supplier<TestCase> supplier) {
            this(name, null, supplier);
        }

        /**
         * Build a test case named after the types it takes.
         */
        public TestCaseSupplier(List<DataType> types, Supplier<TestCase> supplier) {
            this(nameFromTypes(types), types, supplier);
        }

        static String nameFromTypes(List<DataType> types) {
            return types.stream().map(t -> "<" + t.typeName() + ">").collect(Collectors.joining(", "));
        }

        @Override
        public TestCase get() {
            TestCase supplied = supplier.get();
            if (types != null) {
                for (int i = 0; i < types.size(); i++) {
                    if (supplied.data.get(i).type != types.get(i)) {
                        throw new IllegalStateException("supplier/data type mismatch " + supplied.data.get(i).type + "/" + types.get(i));
                    }
                }
            }
            return supplied;
        }

        @Override
        public String toString() {
            return name;
        }

        /**
         * Generate positive test cases for unary functions that operate on an {@code numeric}
         * fields by casting them to {@link DataTypes#DOUBLE}s.
         */
        public static List<TestCaseSupplier> forUnaryCastingToDouble(String name, String argName, DoubleUnaryOperator expected) {
            String read = "Attribute[channel=0]";
            String eval = name + "[" + argName + "=";
            List<TestCaseSupplier> suppliers = new ArrayList<>();
            forUnaryInt(
                suppliers,
                eval + castToDoubleEvaluator(read, DataTypes.INTEGER) + "]",
                DataTypes.DOUBLE,
                i -> expected.applyAsDouble(i)
            );
            forUnaryLong(
                suppliers,
                eval + castToDoubleEvaluator(read, DataTypes.LONG) + "]",
                DataTypes.DOUBLE,
                l -> expected.applyAsDouble(l)
            );
            forUnaryUnsignedLong(
                suppliers,
                eval + castToDoubleEvaluator(read, DataTypes.UNSIGNED_LONG) + "]",
                DataTypes.DOUBLE,
                ul -> expected.applyAsDouble(ul.doubleValue())
            );
            forUnaryDouble(suppliers, eval + read + "]", DataTypes.DOUBLE, i -> expected.applyAsDouble(i));
            return suppliers;
        }

        /**
         * Generate positive test cases for binary functions that operate on an {@code numeric}
         * fields by casting them to {@link DataTypes#DOUBLE}s.
         */
        public static List<TestCaseSupplier> forBinaryCastingToDouble(
            String name,
            String lhsName,
            String rhsName,
            DoubleBinaryOperator expected
        ) {
            List<TestCaseSupplier> suppliers = new ArrayList<>();
            for (DataType lhsType : EsqlDataTypes.types()) {
                if (lhsType.isNumeric() == false || EsqlDataTypes.isRepresentable(lhsType) == false) {
                    continue;
                }
                for (Map.Entry<String, Supplier<Object>> lhsSupplier : RANDOM_VALUE_SUPPLIERS.get(lhsType)) {
                    for (DataType rhsType : EsqlDataTypes.types()) {
                        if (rhsType.isNumeric() == false || EsqlDataTypes.isRepresentable(rhsType) == false) {
                            continue;
                        }
                        for (Map.Entry<String, Supplier<Object>> rhsSupplier : RANDOM_VALUE_SUPPLIERS.get(rhsType)) {
                            String caseName = lhsSupplier.getKey() + ", " + rhsSupplier.getKey();
                            suppliers.add(new TestCaseSupplier(caseName, List.of(lhsType, rhsType), () -> {
                                Number lhs = (Number) lhsSupplier.getValue().get();
                                Number rhs = (Number) rhsSupplier.getValue().get();
                                TypedData lhsTyped = new TypedData(
                                    // TODO there has to be a better way to handle unsigned long
                                    lhs instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : lhs,
                                    lhsType,
                                    "lhs"
                                );
                                TypedData rhsTyped = new TypedData(
                                    rhs instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : rhs,
                                    rhsType,
                                    "rhs"
                                );
                                String lhsEvalName = castToDoubleEvaluator("Attribute[channel=0]", lhsType);
                                String rhsEvalName = castToDoubleEvaluator("Attribute[channel=1]", rhsType);
                                return new TestCase(
                                    List.of(lhsTyped, rhsTyped),
                                    name + "[" + lhsName + "=" + lhsEvalName + ", " + rhsName + "=" + rhsEvalName + "]",
                                    DataTypes.DOUBLE,
                                    equalTo(expected.applyAsDouble(lhs.doubleValue(), rhs.doubleValue()))
                                );
                            }));
                        }
                    }
                }
            }
            return suppliers;
        }

        /**
         * Generate positive test cases for a unary function operating on an {@link DataTypes#INTEGER}.
         */
        public static void forUnaryInt(
            List<TestCaseSupplier> suppliers,
            String expectedEvaluatorToString,
            DataType expectedType,
            IntFunction<Object> expectedValue
        ) {
            unaryNumeric(suppliers, expectedEvaluatorToString, DataTypes.INTEGER, expectedType, n -> expectedValue.apply(n.intValue()));
        }

        /**
         * Generate positive test cases for a unary function operating on an {@link DataTypes#LONG}.
         */
        public static void forUnaryLong(
            List<TestCaseSupplier> suppliers,
            String expectedEvaluatorToString,
            DataType expectedType,
            LongFunction<Object> expectedValue
        ) {
            unaryNumeric(suppliers, expectedEvaluatorToString, DataTypes.LONG, expectedType, n -> expectedValue.apply(n.longValue()));
        }

        /**
         * Generate positive test cases for a unary function operating on an {@link DataTypes#UNSIGNED_LONG}.
         */
        public static void forUnaryUnsignedLong(
            List<TestCaseSupplier> suppliers,
            String expectedEvaluatorToString,
            DataType expectedType,
            Function<BigInteger, Object> expectedValue
        ) {
            unaryNumeric(
                suppliers,
                expectedEvaluatorToString,
                DataTypes.UNSIGNED_LONG,
                expectedType,
                n -> expectedValue.apply((BigInteger) n)
            );
        }

        /**
         * Generate positive test cases for a unary function operating on an {@link DataTypes#DOUBLE}.
         */
        public static void forUnaryDouble(
            List<TestCaseSupplier> suppliers,
            String expectedEvaluatorToString,
            DataType expectedType,
            DoubleFunction<Object> expectedValue
        ) {
            unaryNumeric(suppliers, expectedEvaluatorToString, DataTypes.DOUBLE, expectedType, n -> expectedValue.apply(n.doubleValue()));
        }

        private static void unaryNumeric(
            List<TestCaseSupplier> suppliers,
            String expectedEvaluatorToString,
            DataType inputType,
            DataType expectedOutputType,
            Function<Number, Object> expected
        ) {
            for (Map.Entry<String, Supplier<Object>> supplier : RANDOM_VALUE_SUPPLIERS.get(inputType)) {
                suppliers.add(new TestCaseSupplier(supplier.getKey(), List.of(inputType), () -> {
                    Number value = (Number) supplier.getValue().get();
                    TypedData typed = new TypedData(
                        // TODO there has to be a better way to handle unsigned long
                        value instanceof BigInteger b ? NumericUtils.asLongUnsigned(b) : value,
                        inputType,
                        "value"
                    );
                    return new TestCase(List.of(typed), expectedEvaluatorToString, expectedOutputType, equalTo(expected.apply(value)));
                }));
            }
        }

        private static final Map<DataType, List<Map.Entry<String, Supplier<Object>>>> RANDOM_VALUE_SUPPLIERS = Map.ofEntries(
            Map.entry(
                DataTypes.DOUBLE,
                List.of(
                    Map.entry("<0 double>", () -> 0.0d),
                    Map.entry("<small positive double>", () -> randomDouble()),
                    Map.entry("<small negative double>", () -> -randomDouble()),
                    Map.entry("<big positive double>", () -> randomDoubleBetween(0, Double.MAX_VALUE, false)),
                    Map.entry("<negative positive double>", () -> randomDoubleBetween(Double.MIN_VALUE, 0 - Double.MIN_NORMAL, true))
                )
            ),
            Map.entry(
                DataTypes.LONG,
                List.of(
                    Map.entry("<0 long>", () -> 0L),
                    Map.entry("<positive long>", () -> randomLongBetween(1, Long.MAX_VALUE)),
                    Map.entry("<negative long>", () -> randomLongBetween(Long.MIN_VALUE, -1))
                )
            ),
            Map.entry(
                DataTypes.INTEGER,
                List.of(
                    Map.entry("<0 int>", () -> 0),
                    Map.entry("<positive long>", () -> between(1, Integer.MAX_VALUE)),
                    Map.entry("<negative long>", () -> between(Integer.MIN_VALUE, -1))
                )
            ),
            Map.entry(
                DataTypes.UNSIGNED_LONG,
                List.of(
                    Map.entry("<0 unsigned long>", () -> BigInteger.ZERO),
                    Map.entry("<small unsigned long>", () -> BigInteger.valueOf(randomLongBetween(1, Integer.MAX_VALUE))),
                    Map.entry(
                        "<big unsigned long>",
                        () -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(randomLongBetween(1, Integer.MAX_VALUE)))
                    )
                )
            )
        );

        private static String castToDoubleEvaluator(String original, DataType current) {
            if (current == DataTypes.DOUBLE) {
                return original;
            }
            if (current == DataTypes.INTEGER) {
                return "CastIntToDoubleEvaluator[v=" + original + "]";
            }
            if (current == DataTypes.LONG) {
                return "CastLongToDoubleEvaluator[v=" + original + "]";
            }
            if (current == DataTypes.UNSIGNED_LONG) {
                return "CastUnsignedLongToDoubleEvaluator[v=" + original + "]";
            }
            throw new UnsupportedOperationException();
        }
    }

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
            case "date_period" -> Period.ofDays(randomInt(10));
            case "datetime" -> randomMillisUpToYear9999();
            case "double", "scaled_float" -> randomDouble();
            case "float" -> randomFloat();
            case "half_float" -> HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
            case "keyword" -> new BytesRef(randomAlphaOfLength(5));
            case "ip" -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
            case "time_duration" -> Duration.ofMillis(randomNonNegativeLong());
            case "text" -> new BytesRef(randomAlphaOfLength(50));
            case "version" -> new Version(randomIdentifier()).toBytesRef();
            case "null" -> null;
            default -> throw new IllegalArgumentException("can't make random values for [" + type.typeName() + "]");
        }, type);
    }

    protected TestCase testCase;

    protected static Iterable<Object[]> parameterSuppliersFromTypedData(List<TestCaseSupplier> cases) {
        // TODO rename this method to something more descriptive. Javadoc. And make sure all parameters are "representable" types.
        List<Object[]> parameters = new ArrayList<>(cases.size());
        for (TestCaseSupplier element : cases) {
            parameters.add(new Object[] { element });
        }
        return parameters;
    }

    protected static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true));
    }

    /**
     * Build the expression being tested, for the given source and list of arguments.  Test classes need to implement this
     * to have something to test.
     * @param source the source
     * @param args arg list from the test case, should match the length expected
     * @return an expression for evaluating the function being tested on the given arguments
     */
    protected abstract Expression build(Source source, List<Expression> args);

    protected final Expression buildFieldExpression(TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsFields());
    }

    protected final Expression buildLiteralExpression(TestCase testCase) {
        return build(testCase.getSource(), testCase.getDataAsLiterals());
    }

    protected final Supplier<EvalOperator.ExpressionEvaluator> evaluator(Expression e) {
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
        return new Page(BlockUtils.fromListRow(values));
    }

    /**
     * Hack together a layout by scanning for Fields.
     * Those will show up in the layout in whatever order a depth first traversal finds them.
     */
    protected void buildLayout(Layout.Builder builder, Expression e) {
        if (e instanceof FieldAttribute f) {
            builder.appendChannel(f.id());
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
        Expression expression = buildFieldExpression(testCase);
        if (testCase.expectedTypeError != null) {
            assertTrue("expected unresolved", expression.typeResolved().unresolved());
            assertThat(expression.typeResolved().message(), equalTo(testCase.expectedTypeError));
            return;
        }
        assertFalse("expected resolved", expression.typeResolved().unresolved());
        expression = new FoldNull().rule(expression);
        assertThat(expression.dataType(), equalTo(testCase.expectedType));
        // TODO should we convert unsigned_long into BigDecimal so it's easier to assert?
        Object result = toJavaObject(evaluator(expression).get().eval(row(testCase.getDataValues())), 0);
        assertThat(result, testCase.getMatcher());
        if (testCase.expectedWarnings != null) {
            assertWarnings(testCase.expectedWarnings);
        }
    }

    public final void testSimpleWithNulls() { // TODO replace this with nulls inserted into the test case like anyNullIsNull
        assumeTrue("nothing to do if a type error", testCase.expectedTypeError == null);
        List<Object> simpleData = testCase.getDataValues();
        EvalOperator.ExpressionEvaluator eval = evaluator(buildFieldExpression(testCase)).get();
        Block[] orig = BlockUtils.fromListRow(simpleData);
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
            assertSimpleWithNulls(data, eval.eval(new Page(blocks)), i);
        }
    }

    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        // TODO remove me in favor of cases containing null
        assertTrue("argument " + nullBlock + " is null", value.isNull(0));
    }

    public final void testEvaluateInManyThreads() throws ExecutionException, InterruptedException {
        assumeTrue("nothing to do if a type error", testCase.expectedTypeError == null);
        int count = 10_000;
        int threads = 5;
        Supplier<EvalOperator.ExpressionEvaluator> evalSupplier = evaluator(buildFieldExpression(testCase));
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                List<Object> simpleData = testCase.getDataValues();
                Page page = row(simpleData);

                futures.add(exec.submit(() -> {
                    EvalOperator.ExpressionEvaluator eval = evalSupplier.get();
                    for (int c = 0; c < count; c++) {
                        assertThat(toJavaObject(eval.eval(page), 0), testCase.getMatcher());
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
        assumeTrue("nothing to do if a type error", testCase.expectedTypeError == null);
        var supplier = evaluator(buildFieldExpression(testCase));
        var ev = supplier.get();
        assertThat(ev.toString(), equalTo(testCase.evaluatorToString));
    }

    public final void testFold() {
        Expression expression = buildLiteralExpression(testCase);
        if (testCase.expectedTypeError != null) {
            assertTrue(expression.typeResolved().unresolved());
            assertThat(expression.typeResolved().message(), equalTo(testCase.expectedTypeError));
            return;
        }
        assertFalse(expression.typeResolved().unresolved());
        expression = new FoldNull().rule(expression);
        assertThat(expression.dataType(), equalTo(testCase.expectedType));
        assertTrue(expression.foldable());
        assertThat(expression.fold(), testCase.getMatcher());
        if (testCase.expectedWarnings != null) {
            assertWarnings(testCase.expectedWarnings);
        }
    }

    public void testSerializationOfSimple() {
        assertSerialization(buildFieldExpression(testCase));
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
        for (TestCaseSupplier s : testCaseSuppliers) {
            if (s.types == null) {
                throw new IllegalArgumentException("types required");
            }
        }
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
            boolean firstTimeSeenSignature = uniqueSignatures.add(original.types);
            for (int nullPosition = 0; nullPosition < original.types.size(); nullPosition++) {
                int finalNullPosition = nullPosition;
                suppliers.add(new TestCaseSupplier(original.name + " null in " + nullPosition, original.types, () -> {
                    TestCase oc = original.get();
                    List<TypedData> data = IntStream.range(0, oc.data.size()).mapToObj(i -> {
                        TypedData od = oc.data.get(i);
                        if (i == finalNullPosition) {
                            return new TypedData(null, od.type, od.name);
                        }
                        return od;
                    }).toList();
                    return new TestCase(
                        data,
                        oc.evaluatorToString,
                        oc.expectedType,
                        nullValue(),
                        oc.expectedWarnings,
                        oc.expectedTypeError
                    );
                }));

                if (firstTimeSeenSignature) {
                    List<DataType> typesWithNull = IntStream.range(0, original.types.size())
                        .mapToObj(i -> i == finalNullPosition ? DataTypes.NULL : original.types.get(i))
                        .toList();
                    boolean newSignature = uniqueSignatures.add(typesWithNull);
                    if (newSignature) {
                        suppliers.add(new TestCaseSupplier(typesWithNull, () -> {
                            TestCase oc = original.get();
                            List<TypedData> data = IntStream.range(0, oc.data.size()).mapToObj(i -> {
                                TypedData od = oc.data.get(i);
                                if (i == finalNullPosition) {
                                    return new TypedData(null, DataTypes.NULL, od.name);
                                }
                                return od;
                            }).toList();
                            return new TestCase(
                                data,
                                "LiteralsEvaluator[block=null]",
                                entirelyNullPreservesType == false && oc.data.size() == 1 ? DataTypes.NULL : oc.expectedType,
                                nullValue(),
                                oc.expectedWarnings,
                                oc.expectedTypeError
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
        for (TestCaseSupplier s : testCaseSuppliers) {
            if (s.types == null) {
                throw new IllegalArgumentException("types required");
            }
        }
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
            .map(types -> typeErrorSupplier(validPerPosition, types))
            .forEach(suppliers::add);
        return suppliers;
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
    private static TestCaseSupplier typeErrorSupplier(List<Set<DataType>> validPerPosition, List<DataType> types) {
        return new TestCaseSupplier(
            "type error for " + TestCaseSupplier.nameFromTypes(types),
            types,
            () -> TestCase.typeError(
                types.stream().map(type -> new TypedData(randomLiteral(type).value(), type, type.typeName())).toList(),
                typeErrorMessage(validPerPosition, types)
            )
        );
    }

    /**
     * Build the expected error message for an invalid type signature.
     */
    private static String typeErrorMessage(List<Set<DataType>> validPerPosition, List<DataType> types) {
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
        String ordinal = validPerPosition.size() == 1
            ? ""
            : TypeResolutions.ParamOrdinal.fromIndex(badArgPosition).name().toLowerCase(Locale.ROOT) + " ";
        String expectedType = expectedType(validPerPosition.get(badArgPosition));
        String name = types.get(badArgPosition).typeName();
        return ordinal + "argument of [] must be [" + expectedType + "], found value [" + name + "] type [" + name + "]";
    }

    private static final Map<Set<DataType>, String> NAMED_EXPECTED_TYPES = Map.ofEntries(
        Map.entry(Set.of(DataTypes.DOUBLE, DataTypes.NULL), "double"),
        Map.entry(Set.of(DataTypes.INTEGER, DataTypes.NULL), "integer"),
        Map.entry(Set.of(DataTypes.LONG, DataTypes.INTEGER, DataTypes.UNSIGNED_LONG, DataTypes.DOUBLE, DataTypes.NULL), "numeric")
    );

    private static String expectedType(Set<DataType> validTypes) {
        String named = NAMED_EXPECTED_TYPES.get(validTypes);
        if (named == null) {
            /*
             * Note for anyone who's test lands here - it's likely that you
             * don't have a test case covering explicit `null` arguments in
             * this position. Generally you can get that with anyNullIsNull.
             */
            throw new UnsupportedOperationException("can't guess expected types for " + validTypes);
        }
        return named;
    }

    private static Stream<DataType> representable() {
        return EsqlDataTypes.types().stream().filter(EsqlDataTypes::isRepresentable);
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
        if (testCase.expectedTypeError != null) {
            return;
        }
        if (testCase.getData().stream().anyMatch(t -> t.type == DataTypes.NULL)) {
            return;
        }
        signatures.putIfAbsent(testCase.getData().stream().map(TypedData::type).toList(), testCase.expectedType);
    }

    @AfterClass
    public static void renderTypesTable() throws IOException {
        FunctionDefinition definition = definition();
        if (definition == null) {
            LogManager.getLogger(getTestClass()).info("Skipping rendering types because the function isn't registered");
            return;
        }

        List<String> definedSignature = ShowFunctions.signature(definition);
        StringBuilder header = new StringBuilder();
        for (String arg : definedSignature) {
            header.append(arg).append(" | ");
        }
        header.append("result");

        List<String> table = new ArrayList<>();
        for (Map.Entry<List<DataType>, DataType> sig : signatures.entrySet()) {
            if (sig.getKey().size() != definedSignature.size()) {
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
        return getTestClass().getSimpleName().replace("Tests", "").toLowerCase(Locale.ROOT);
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
}
