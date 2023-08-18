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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.EvalMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.hamcrest.Matchers.equalTo;

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

        public TestCase(List<TypedData> data, String evaluatorToString, DataType expectedType, Matcher<Object> matcher) {
            this.source = Source.EMPTY;
            this.data = data;
            this.evaluatorToString = evaluatorToString;
            this.expectedType = expectedType;
            this.matcher = matcher;
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
            return data.stream().map(t -> new Literal(source, t.data(), t.type())).collect(Collectors.toList());
        }

        public List<Object> getDataValues() {
            return data.stream().map(t -> t.data()).collect(Collectors.toList());
        }

        public Matcher<Object> getMatcher() {
            return matcher;
        }
    }

    /**
     * This class exists to give a human-readable string representation of the test case.
     */
    protected static class TestCaseSupplier implements Supplier<TestCase> {

        private String name;
        private final Supplier<TestCase> wrapped;

        public TestCaseSupplier(String name, Supplier<TestCase> wrapped) {
            this.name = name;
            this.wrapped = wrapped;
        }

        @Override
        public TestCase get() {
            return wrapped.get();
        }

        @Override
        public String toString() {
            return name;
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
        if (e.foldable()) {
            e = new Literal(e.source(), e.fold(), e.dataType());
        }
        Layout.Builder builder = new Layout.Builder();
        buildLayout(builder, e);
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

    public final void testSimple() {
        Expression expression = buildFieldExpression(testCase);
        assertThat(expression.dataType(), equalTo(testCase.expectedType));
        // TODO should we convert unsigned_long into BigDecimal so it's easier to assert?
        Object result = toJavaObject(evaluator(expression).get().eval(row(testCase.getDataValues())), 0);
        assertThat(result, testCase.getMatcher());
    }

    public final void testSimpleWithNulls() {
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
        assertTrue("argument " + nullBlock + " is null", value.isNull(0));
    }

    public final void testSimpleInManyThreads() throws ExecutionException, InterruptedException {
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

    public final void testEvaluatorSimpleToString() {
        assertThat(evaluator(buildFieldExpression(testCase)).get().toString(), equalTo(testCase.evaluatorToString));
    }

    public final void testSimpleConstantFolding() {
        Expression e = buildLiteralExpression(testCase);
        assertThat(e.dataType(), equalTo(testCase.expectedType));
        assertTrue(e.foldable());
        assertThat(e.fold(), testCase.getMatcher());
    }

    public void testSerializationOfSimple() {
        assertSerialization(buildFieldExpression(testCase));
    }
}
