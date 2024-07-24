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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.session.Configuration;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.FoldNull;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.After;
import org.junit.AfterClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Base class for function tests.
 */
public abstract class AbstractFunctionTestCase extends ESTestCase {
    /**
     * Operators are unregistered functions.
     */
    private static final Map<String, Class<?>> OPERATORS = Map.ofEntries(
        entry("in", In.class),
        entry("like", WildcardLike.class),
        entry("rlike", RLike.class),
        entry("equals", Equals.class),
        entry("not_equals", NotEquals.class),
        entry("greater_than", GreaterThan.class),
        entry("greater_than_or_equal", GreaterThanOrEqual.class),
        entry("less_than", LessThan.class),
        entry("less_than_or_equal", LessThanOrEqual.class),
        entry("add", Add.class),
        entry("sub", Sub.class),
        entry("mul", Mul.class),
        entry("div", Div.class),
        entry("mod", Mod.class),
        entry("neg", Neg.class),
        entry("is_null", IsNull.class),
        entry("is_not_null", IsNotNull.class)
    );

    /**
     * Generate a random value of the appropriate type to fit into blocks of {@code e}.
     */
    public static Literal randomLiteral(DataType type) {
        return new Literal(Source.EMPTY, switch (type) {
            case BOOLEAN -> randomBoolean();
            case BYTE -> randomByte();
            case SHORT -> randomShort();
            case INTEGER, COUNTER_INTEGER -> randomInt();
            case UNSIGNED_LONG, LONG, COUNTER_LONG -> randomLong();
            case DATE_PERIOD -> Period.of(randomIntBetween(-1000, 1000), randomIntBetween(-13, 13), randomIntBetween(-32, 32));
            case DATETIME -> randomMillisUpToYear9999();
            case DOUBLE, SCALED_FLOAT, COUNTER_DOUBLE -> randomDouble();
            case FLOAT -> randomFloat();
            case HALF_FLOAT -> HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
            case KEYWORD -> new BytesRef(randomAlphaOfLength(5));
            case IP -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
            case TIME_DURATION -> Duration.ofMillis(randomLongBetween(-604800000L, 604800000L)); // plus/minus 7 days
            case TEXT -> new BytesRef(randomAlphaOfLength(50));
            case VERSION -> randomVersion().toBytesRef();
            case GEO_POINT -> GEO.asWkb(GeometryTestUtils.randomPoint());
            case CARTESIAN_POINT -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint());
            case GEO_SHAPE -> GEO.asWkb(GeometryTestUtils.randomGeometry(randomBoolean()));
            case CARTESIAN_SHAPE -> CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(randomBoolean()));
            case NULL -> null;
            case SOURCE -> {
                try {
                    yield BytesReference.bytes(
                        JsonXContent.contentBuilder().startObject().field(randomAlphaOfLength(3), randomAlphaOfLength(10)).endObject()
                    ).toBytesRef();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            case UNSUPPORTED, OBJECT, NESTED, DOC_DATA_TYPE, TSID_DATA_TYPE, PARTIAL_AGG -> throw new IllegalArgumentException(
                "can't make random values for [" + type.typeName() + "]"
            );
        }, type);
    }

    protected TestCaseSupplier.TestCase testCase;

    /**
     * Converts typed test suppliers to parameterized test parameters.
     * <p>
     *     Use {@code parameterSuppliersFromTypedDataWithDefaultChecks()} instead if possible, as it automatically add default checks.
     * </p>
     */
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
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true));
    }

    /**
     * Build an {@link Attribute} that loads a field and then creates a deep copy of its data.
     */
    public static Expression deepCopyOfField(String name, DataType type) {
        return new DeepCopy(
            Source.synthetic(name),
            new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true))
        );
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
        assumeTrue("Data can't be converted to literals", testCase.canGetDataAsLiterals());
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
     * Creates a list of pages based on a list of multi-row fields.
     */
    protected final List<Page> rows(List<TestCaseSupplier.TypedData> multirowFields) {
        if (multirowFields.isEmpty()) {
            return List.of();
        }

        var rowsCount = multirowFields.get(0).multiRowData().size();

        multirowFields.stream()
            .skip(1)
            .forEach(
                field -> assertThat("All multi-row fields must have the same number of rows", field.multiRowData(), hasSize(rowsCount))
            );

        List<Page> pages = new ArrayList<>();

        int pageSize = randomIntBetween(1, 100);
        for (int initialRow = 0; initialRow < rowsCount;) {
            if (pageSize > rowsCount - initialRow) {
                pageSize = rowsCount - initialRow;
            }

            var blocks = new Block[multirowFields.size()];

            for (int i = 0; i < multirowFields.size(); i++) {
                var field = multirowFields.get(i);
                try (
                    var wrapper = BlockUtils.wrapperFor(
                        TestBlockFactory.getNonBreakingInstance(),
                        PlannerUtils.toElementType(field.type()),
                        pageSize
                    )
                ) {
                    var multiRowData = field.multiRowData();
                    for (int row = initialRow; row < initialRow + pageSize; row++) {
                        wrapper.accept(multiRowData.get(row));
                    }

                    blocks[i] = wrapper.builder().build();
                }
            }

            pages.add(new Page(pageSize, blocks));
            initialRow += pageSize;
            pageSize = randomIntBetween(1, 100);
        }

        return pages;
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

    protected Object toJavaObjectUnsignedLongAware(Block block, int position) {
        Object result;
        result = toJavaObject(block, position);
        if (result != null && testCase.expectedType() == DataType.UNSIGNED_LONG) {
            assertThat(result, instanceOf(Long.class));
            result = NumericUtils.unsignedLongAsBigInteger((Long) result);
        }
        return result;
    }

    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        // TODO remove me in favor of cases containing null
        assertTrue("argument " + nullBlock + " is null", value.isNull(0));
    }

    /**
     * Modifies suppliers to generate BytesRefs with random offsets.
     */
    protected static List<TestCaseSupplier> randomizeBytesRefsOffset(List<TestCaseSupplier> testCaseSuppliers) {
        return testCaseSuppliers.stream().map(supplier -> new TestCaseSupplier(supplier.name(), supplier.types(), () -> {
            var testCase = supplier.supplier().get();

            var newData = testCase.getData().stream().map(typedData -> {
                if (typedData.isMultiRow()) {
                    return typedData.withData(
                        typedData.multiRowData().stream().map(AbstractFunctionTestCase::tryRandomizeBytesRefOffset).toList()
                    );
                }

                return typedData.withData(tryRandomizeBytesRefOffset(typedData.data()));
            }).toList();

            return new TestCaseSupplier.TestCase(
                newData,
                testCase.evaluatorToString(),
                testCase.expectedType(),
                testCase.getMatcher(),
                testCase.getExpectedWarnings(),
                testCase.getExpectedTypeError(),
                testCase.foldingExceptionClass(),
                testCase.foldingExceptionMessage()
            );
        })).toList();
    }

    private static Object tryRandomizeBytesRefOffset(Object value) {
        if (value instanceof BytesRef bytesRef) {
            return randomizeBytesRefOffset(bytesRef);
        }

        if (value instanceof List<?> list) {
            return list.stream().map(element -> {
                if (element instanceof BytesRef bytesRef) {
                    return randomizeBytesRefOffset(bytesRef);
                }
                return element;
            }).toList();
        }

        return value;
    }

    private static BytesRef randomizeBytesRefOffset(BytesRef bytesRef) {
        var offset = randomIntBetween(0, 10);
        var extraLength = randomIntBetween(0, 10);
        var newBytesArray = randomByteArrayOfLength(bytesRef.length + offset + extraLength);

        System.arraycopy(bytesRef.bytes, bytesRef.offset, newBytesArray, offset, bytesRef.length);

        return new BytesRef(newBytesArray, offset, bytesRef.length);
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
        assertThat(
            "descriptions should be complete sentences",
            description.description(),
            either(endsWith(".")) // A full sentence
                .or(endsWith("∅")) // Math
        );

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

    protected final void assertTypeResolutionFailure(Expression expression) {
        assertTrue("expected unresolved", expression.typeResolved().unresolved());
        assertThat(expression.typeResolved().message(), equalTo(testCase.getExpectedTypeError()));
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
            if (tc.getData().stream().anyMatch(t -> t.type() == DataType.NULL)) {
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
        if (binaryOperator(name) != null || unaryOperator(name) != null || likeOrInOperator(name)) {
            renderDocsForOperators(name);
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
            boolean hasAppendix = renderAppendix(info.appendix());
            renderFullLayout(name, hasExamples, hasAppendix);
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

    private static boolean renderAppendix(String appendix) throws IOException {
        if (appendix.isEmpty()) {
            return false;
        }

        String rendered = DOCS_WARNING + appendix + "\n";

        LogManager.getLogger(getTestClass()).info("Writing appendix for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("appendix", rendered, "asciidoc");
        return true;
    }

    private static void renderFullLayout(String name, boolean hasExamples, boolean hasAppendix) throws IOException {
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
        if (hasAppendix) {
            rendered += "include::../appendix/" + name + ".asciidoc[]\n";
        }
        LogManager.getLogger(getTestClass()).info("Writing layout for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("layout", rendered, "asciidoc");
    }

    private static Constructor<?> constructorWithFunctionInfo(Class<?> clazz) {
        for (Constructor<?> ctor : clazz.getConstructors()) {
            FunctionInfo functionInfo = ctor.getAnnotation(FunctionInfo.class);
            if (functionInfo != null) {
                return ctor;
            }
        }
        return null;
    }

    private static void renderDocsForOperators(String name) throws IOException {
        Constructor<?> ctor = constructorWithFunctionInfo(OPERATORS.get(name));
        assert ctor != null;
        FunctionInfo functionInfo = ctor.getAnnotation(FunctionInfo.class);
        assert functionInfo != null;
        renderKibanaInlineDocs(name, functionInfo);

        var params = ctor.getParameters();

        List<EsqlFunctionRegistry.ArgSignature> args = new ArrayList<>(params.length);
        for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
            if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
                Param paramInfo = params[i].getAnnotation(Param.class);
                String paramName = paramInfo == null ? params[i].getName() : paramInfo.name();
                String[] type = paramInfo == null ? new String[] { "?" } : paramInfo.type();
                String desc = paramInfo == null ? "" : paramInfo.description().replace('\n', ' ');
                boolean optional = paramInfo == null ? false : paramInfo.optional();
                DataType targetDataType = EsqlFunctionRegistry.getTargetType(type);
                args.add(new EsqlFunctionRegistry.ArgSignature(paramName, type, desc, optional, targetDataType));
            }
        }
        renderKibanaFunctionDefinition(name, functionInfo, args, likeOrInOperator(name));
        renderTypes(args.stream().map(EsqlFunctionRegistry.ArgSignature::name).toList());
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
        builder.field("type", isAggregation() ? "agg" : "eval");
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
     * If this tests is for a like or rlike operator return true, otherwise return {@code null}.
     */
    private static boolean likeOrInOperator(String name) {
        return name.equalsIgnoreCase("rlike") || name.equalsIgnoreCase("like") || name.equalsIgnoreCase("in");
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
        return DataType.types().stream().filter(DataType::isString).toArray(DataType[]::new);
    }

    /**
     * Returns true if the current test case is for an aggregation function.
     * <p>
     *     This method requires reflection, as it's called from a static context (@AfterClass documentation rendering).
     * </p>
     */
    private static boolean isAggregation() {
        return AbstractAggregationTestCase.class.isAssignableFrom(getTestClass());
    }
}
