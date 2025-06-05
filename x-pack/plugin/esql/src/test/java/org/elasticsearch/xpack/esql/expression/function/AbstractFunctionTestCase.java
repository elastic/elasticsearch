/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import com.carrotsearch.randomizedtesting.ClassModel;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base class for function tests.
 */
public abstract class AbstractFunctionTestCase extends ESTestCase {

    private static EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry().snapshotRegistry();

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
     * Adds cases with {@code null} and asserts that the result is {@code null}.
     * <p>
     * Note: This won't add more than a single null to any existing test case,
     * just to keep the number of test cases from exploding totally.
     * </p>
     *
     * @param entirelyNullPreservesType should a test case that only contains parameters
     *                                  with the {@code null} type keep it’s expected type?
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
            (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original
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
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers.size());
        suppliers.addAll(testCaseSuppliers);

        /*
         * For each original test case, add as many copies as there were
         * arguments, replacing one of the arguments with null and keeping
         * the others.
         *
         * Also, if this was the first time we saw the signature we copy it
         * *again*, replacing the argument with null, but annotating the
         * argument’s type as `null` explicitly.
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
                        if (i != finalNullPosition) {
                            return od;
                        }
                        return od.withData(od.isMultiRow() ? Collections.singletonList(null) : null);
                    }).toList();
                    TestCaseSupplier.TypedData nulledData = oc.getData().get(finalNullPosition);
                    return new TestCaseSupplier.TestCase(
                        data,
                        evaluatorToString.evaluatorToString(finalNullPosition, nulledData, oc.evaluatorToString()),
                        expectedType.expectedType(finalNullPosition, nulledData.type(), oc),
                        nullValue(),
                        null,
                        null,
                        oc.getExpectedTypeError(),
                        null,
                        null,
                        null,
                        oc.canBuildEvaluator()
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
                                .mapToObj(
                                    i -> i == finalNullPosition
                                        ? (oc.getData().get(i).isMultiRow()
                                            ? TestCaseSupplier.TypedData.MULTI_ROW_NULL
                                            : TestCaseSupplier.TypedData.NULL)
                                        : oc.getData().get(i)
                                )
                                .toList();
                            return new TestCaseSupplier.TestCase(
                                data,
                                equalTo("LiteralsEvaluator[lit=null]"),
                                expectedType.expectedType(finalNullPosition, DataType.NULL, oc),
                                nullValue(),
                                null,
                                null,
                                oc.getExpectedTypeError(),
                                null,
                                null,
                                null,
                                oc.canBuildEvaluator()
                            );
                        }));
                    }
                }
            }
        }

        return suppliers;
    }

    @FunctionalInterface
    public interface PositionalErrorMessageSupplier {
        /**
         * This interface defines functions to supply error messages for incorrect types in specific positions. Functions which have
         * the same type requirements for all positions can simplify this with a lambda returning a string constant.
         *
         * @param validForPosition - the set of {@link DataType}s that the test infrastructure believes to be allowable in the
         *                         given position.
         * @param position - the zero-index position in the list of parameters the function has detected the bad argument to be.
         * @return The string describing the acceptable parameters for that position.  Note that this function should not return
         *         the full error string; that will be constructed by the test.  Just return the type string for that position.
         */
        String apply(Set<DataType> validForPosition, int position);
    }

    /**
     * Adds test cases containing unsupported parameter types that assert
     * that they throw type errors.
     * @deprecated make a subclass of {@link ErrorsForCasesWithoutExamplesTestCase} instead
     */
    @Deprecated
    protected static List<TestCaseSupplier> errorsForCasesWithoutExamples(
        List<TestCaseSupplier> testCaseSuppliers,
        PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        return errorsForCasesWithoutExamples(testCaseSuppliers, (i, v, t) -> typeErrorMessage(i, v, t, positionalErrorMessageSupplier));
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
        return ordinal + "argument of [source] must be [" + expectedTypeString + "], found value [" + name + "] type [" + name + "]";
    }

    @FunctionalInterface
    protected interface TypeErrorMessageSupplier {
        String apply(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types);
    }

    /**
     * @deprecated make a subclass of {@link ErrorsForCasesWithoutExamplesTestCase} instead
     */
    @Deprecated
    protected static List<TestCaseSupplier> errorsForCasesWithoutExamples(
        List<TestCaseSupplier> testCaseSuppliers,
        TypeErrorMessageSupplier typeErrorMessageSupplier
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers.size());
        suppliers.addAll(testCaseSuppliers);

        Set<List<DataType>> valid = testCaseSuppliers.stream().map(TestCaseSupplier::types).collect(Collectors.toSet());
        List<Set<DataType>> validPerPosition = validPerPosition(valid);

        testCaseSuppliers.stream()
            .map(s -> s.types().size())
            .collect(Collectors.toSet())
            .stream()
            .flatMap(AbstractFunctionTestCase::allPermutations)
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

    private static List<DataType> append(List<DataType> orig, DataType extra) {
        List<DataType> longer = new ArrayList<>(orig.size() + 1);
        longer.addAll(orig);
        longer.add(extra);
        return longer;
    }

    protected static TestCaseSupplier typeErrorSupplier(
        boolean includeOrdinal,
        List<Set<DataType>> validPerPosition,
        List<DataType> types,
        PositionalErrorMessageSupplier errorMessageSupplier
    ) {
        return typeErrorSupplier(includeOrdinal, validPerPosition, types, (o, v, t) -> typeErrorMessage(o, v, t, errorMessageSupplier));
    }

    /**
     * Build a test case that asserts that the combination of parameter types is an error.
     * @deprecated use an extension of {@link ErrorsForCasesWithoutExamplesTestCase}
     */
    @Deprecated
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

    static List<Set<DataType>> validPerPosition(Set<List<DataType>> valid) {
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

    protected static Stream<List<DataType>> allPermutations(int argumentCount) {
        if (argumentCount == 0) {
            return Stream.of(List.of());
        }
        if (argumentCount > 3) {
            throw new IllegalArgumentException("would generate too many combinations");
        }
        Stream<List<DataType>> stream = validFunctionParameters().map(List::of);
        for (int i = 1; i < argumentCount; i++) {
            stream = stream.flatMap(types -> validFunctionParameters().map(t -> append(types, t)));
        }
        return stream;
    }

    /**
     * The types that are valid in function parameters. This is used by the
     * function tests to enumerate all possible parameters to test error messages
     * for invalid combinations.
     */
    public static Stream<DataType> validFunctionParameters() {
        return Arrays.stream(DataType.values()).filter(t -> {
            if (t == DataType.UNSUPPORTED) {
                // By definition, functions never support UNSUPPORTED
                return false;
            }
            if (t == DataType.DOC_DATA_TYPE || t == DataType.PARTIAL_AGG) {
                /*
                 * Doc and partial_agg are special and functions aren't
                 * defined to take these. They'll use them implicitly if needed.
                 */
                return false;
            }
            if (t == DataType.OBJECT) {
                // Object and nested fields aren't supported by any functions yet
                return false;
            }
            if (t == DataType.SOURCE || t == DataType.TSID_DATA_TYPE) {
                // No functions take source or tsid fields yet. We'll make some eventually and remove this.
                return false;
            }
            if (t == DataType.DATE_PERIOD || t == DataType.TIME_DURATION) {
                // We don't test that functions don't take date_period or time_duration. We should.
                return false;
            }
            if (DataType.UNDER_CONSTRUCTION.containsKey(t)) {
                /*
                 * Types under construction aren't checked because we're actively
                 * adding support for them to functions. That’s *why* they are
                 * under construction.
                 */
                return false;
            }
            if (t.isCounter()) {
                /*
                 * For now, we're assuming no functions take counters
                 * as parameters. That’s not true - some do. But we'll
                 * need to update the tests to handle that.
                 */
                return false;
            }
            if (t.widenSmallNumeric() != t) {
                // Small numeric types are widened long before they arrive at functions.
                return false;
            }

            return true;
        }).sorted();
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
        return randomSerializeDeserialize(build(testCase.getSource(), testCase.getDataAsFields()));
    }

    /**
     * Build an {@link Expression} where all inputs are anonymous functions
     * that make a copy of the values from a field <strong>except</strong>
     * those that have been marked with {@link TestCaseSupplier.TypedData#forceLiteral()}.
     */
    protected final Expression buildDeepCopyOfFieldExpression(TestCaseSupplier.TestCase testCase) {
        // We don't use `randomSerializeDeserialize()` here as the deep copied fields aren't deserializable right now
        return build(testCase.getSource(), testCase.getDataAsDeepCopiedFields());
    }

    private Expression randomSerializeDeserialize(Expression expression) {
        if (canSerialize() == false || randomBoolean()) {
            return expression;
        }

        return serializeDeserializeExpression(expression);
    }

    /**
     * The expression being tested be serialized? The <strong>vast</strong>
     * majority of expressions can be serialized.
     */
    protected boolean canSerialize() {
        return true;
    }

    /**
     * Returns the expression after being serialized and deserialized.
     * <p>
     *     Tests randomly go through this method to ensure that the function retains the same logic after serialization and deserialization.
     * </p>
     * <p>
     *     Can be overridden to provide custom serialization and deserialization logic, or disable it if needed.
     * </p>
    */
    protected Expression serializeDeserializeExpression(Expression expression) {
        Expression newExpression = serializeDeserialize(
            expression,
            PlanStreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(Expression.class),
            testCase.getConfiguration() // The configuration query should be == to the source text of the function for this to work
        );

        // Fields use synthetic sources, which can't be serialized. So we replace with the originals instead.
        var dummyChildren = newExpression.children()
            .stream()
            .<Expression>map(c -> new Literal(Source.EMPTY, "anything that won't match any test case", c.dataType()))
            .toList();
        // We first replace them with other unrelated expressions to force a replace, as some replaceChildren() will check for equality
        return newExpression.replaceChildrenSameSize(dummyChildren).replaceChildrenSameSize(expression.children());
    }

    protected final Expression buildLiteralExpression(TestCaseSupplier.TestCase testCase) {
        assumeTrue("Data can't be converted to literals", testCase.canGetDataAsLiterals());
        return randomSerializeDeserialize(build(testCase.getSource(), testCase.getDataAsLiterals()));
    }

    public static EvaluatorMapper.ToEvaluator toEvaluator() {
        return new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return evaluator(expression);
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };
    }

    /**
     * Convert an {@link Expression} tree into a {@link ExpressionEvaluator.Factory}
     * for {@link ExpressionEvaluator}s in the same way as our planner.
     */
    public static ExpressionEvaluator.Factory evaluator(Expression e) {
        e = new FoldNull().rule(e, unboundLogicalOptimizerContext());
        if (e.foldable()) {
            e = new Literal(e.source(), e.fold(FoldContext.small()), e.dataType());
        }
        if (e instanceof SurrogateExpression s) {
            Expression surrogate = s.surrogate();
            if (surrogate != null) {
                e = surrogate;
            }
        }
        Layout.Builder builder = new Layout.Builder();
        buildLayout(builder, e);
        Expression.TypeResolution resolution = e.typeResolved();
        if (resolution.unresolved()) {
            throw new AssertionError("expected resolved " + resolution.message());
        }
        return EvalMapper.toEvaluator(FoldContext.small(), e, builder.build());
    }

    protected final Page row(List<Object> values) {
        return maybeConvertBytesRefsToOrdinals(new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values)));
    }

    private Page maybeConvertBytesRefsToOrdinals(Page page) {
        boolean anyBytesRef = false;
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b).elementType() == ElementType.BYTES_REF) {
                anyBytesRef = true;
                break;
            }
        }
        return anyBytesRef && randomBoolean() ? BlockTestUtils.convertBytesRefsToOrdinals(page) : page;
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
                        var data = multiRowData.get(row);
                        if (data instanceof BigInteger bigIntegerData) {
                            wrapper.accept(NumericUtils.asLongUnsigned(bigIntegerData));
                        } else {
                            wrapper.accept(data);
                        }
                    }

                    blocks[i] = wrapper.builder().build();
                }
            }

            pages.add(maybeConvertBytesRefsToOrdinals(new Page(pageSize, blocks)));
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
        if (result == null || testCase.expectedType() != DataType.UNSIGNED_LONG) {
            return result;
        }
        if (result instanceof List<?> l) {
            return l.stream().map(v -> {
                assertThat(v, instanceOf(Long.class));
                return NumericUtils.unsignedLongAsBigInteger((Long) v);
            }).toList();
        }
        assertThat(result, instanceOf(Long.class));
        return NumericUtils.unsignedLongAsBigInteger((Long) result);
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

            return testCase.withData(newData);
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
        assumeTrue("can't serialize function", canSerialize());
        assertSerialization(buildFieldExpression(testCase), testCase.getConfiguration());
    }

    /**
     * This test is meant to validate that the params annotations for the function being tested align with the supported types the
     * test framework has detected.
     */
    @AfterClass
    public static void testFunctionInfo() {
        Class<?> testClass = getTestClass();
        Logger log = LogManager.getLogger(testClass);
        FunctionDefinition definition = definition(functionName());
        if (definition == null) {
            log.info("Skipping function info checks because the function isn't registered");
            return;
        }
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
        Set<String> returnFromSignature = new TreeSet<>();
        for (int i = 0; i < args.size(); i++) {
            typesFromSignature.add(new HashSet<>());
        }
        for (Map.Entry<List<DataType>, DataType> entry : signatures(testClass).entrySet()) {
            List<DataType> types = entry.getKey();
            for (int i = 0; i < args.size() && i < types.size(); i++) {
                typesFromSignature.get(i).add(types.get(i).esNameIfPossible());
            }
            if (DataType.UNDER_CONSTRUCTION.containsKey(entry.getValue()) == false) {
                returnFromSignature.add(entry.getValue().esNameIfPossible());
            }
        }

        for (int i = 0; i < args.size(); i++) {
            EsqlFunctionRegistry.ArgSignature arg = args.get(i);
            Set<String> annotationTypes = Arrays.stream(arg.type())
                .filter(t -> DataType.UNDER_CONSTRUCTION.containsKey(DataType.fromNameOrAlias(t)) == false)
                .collect(Collectors.toCollection(TreeSet::new));
            Set<String> signatureTypes = typesFromSignature.get(i)
                .stream()
                .filter(t -> DataType.UNDER_CONSTRUCTION.containsKey(DataType.fromNameOrAlias(t)) == false)
                .collect(Collectors.toCollection(TreeSet::new));
            if (signatureTypes.isEmpty()) {
                log.info("{}: skipping", arg.name());
                continue;
            }
            log.info("{}: tested {} vs annotated {}", arg.name(), signatureTypes, annotationTypes);
            if (annotationTypes.size() == 1 && annotationTypes.iterator().next().equalsIgnoreCase("map")) { // map is not a DataType
                continue;
            }
            assertEquals(
                "Mismatch between actual and declared param type for ["
                    + arg.name()
                    + "]. "
                    + "You probably need to update your @params annotations or add test cases to your test.",
                signatureTypes,
                annotationTypes
            );
        }

        Set<String> returnTypes = Arrays.stream(description.returnType())
            .filter(t -> DataType.UNDER_CONSTRUCTION.containsKey(DataType.fromNameOrAlias(t)) == false)
            .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(returnFromSignature, returnTypes);
    }

    /**
     * This test is meant to validate that the license checks documented match those enforced.
     * The expectations are set in the test class using a method with this signature:
     * <code>
     *     public static License.OperationMode licenseRequirement(List&lt;DataType&gt; fieldTypes);
     * </code>
     * License enforcement in the function class is achieved using the interface <code>LicenseAware</code>.
     * This test will make sure the two are in agreement, and does not require that the function class actually
     * report its license level. If we add license checks to any function, but fail to also add the expected
     * license level to the test class, this test will fail.
     */
    @AfterClass
    public static void testFunctionLicenseChecks() throws Exception {
        Class<?> testClass = getTestClass();
        Logger log = LogManager.getLogger(testClass);
        FunctionDefinition definition = definition(functionName());
        if (definition == null) {
            log.info("Skipping function info checks because the function isn't registered");
            return;
        }
        log.info("Running function license checks");
        DocsV3Support.LicenseRequirementChecker licenseChecker = new DocsV3Support.LicenseRequirementChecker(testClass);
        License.OperationMode functionLicense = licenseChecker.invoke(null);
        Constructor<?> ctor = constructorWithFunctionInfo(definition.clazz());
        if (LicenseAware.class.isAssignableFrom(definition.clazz()) == false) {
            // Perform simpler no-signature tests
            assertThat(
                "Function " + definition.name() + " should be licensed under " + functionLicense,
                functionLicense,
                equalTo(License.OperationMode.BASIC)
            );
            return;
        }
        // For classes with LicenseAware, we need to check that the license is correct
        TestCheckLicense checkLicense = new TestCheckLicense();

        // Go through all signatures and assert that the license is as expected
        signatures(testClass).forEach((signature, returnType) -> {
            try {
                License.OperationMode license = licenseChecker.invoke(signature);
                assertNotNull("License should not be null", license);

                // Construct an instance of the class and then call it's licenseCheck method, and compare the results
                Object[] args = new Object[ctor.getParameterCount()];
                args[0] = Source.EMPTY;
                for (int i = 0; i < signature.size(); i++) {
                    args[i + 1] = new Literal(Source.EMPTY, null, signature.get(i));
                }
                Object instance = ctor.newInstance(args);
                // Check that object implements the LicenseAware interface
                if (LicenseAware.class.isAssignableFrom(instance.getClass())) {
                    LicenseAware licenseAware = (LicenseAware) instance;
                    switch (license) {
                        case BASIC -> checkLicense.assertLicenseCheck(licenseAware, signature, true, true, true);
                        case PLATINUM -> checkLicense.assertLicenseCheck(licenseAware, signature, false, true, true);
                        case ENTERPRISE -> checkLicense.assertLicenseCheck(licenseAware, signature, false, false, true);
                    }
                } else {
                    fail("Function " + definition.name() + " does not implement LicenseAware");
                }
            } catch (Exception e) {
                fail(e);
            }
        });
    }

    private static class TestCheckLicense {
        XPackLicenseState basicLicense = makeLicenseState(License.OperationMode.BASIC);
        XPackLicenseState platinumLicense = makeLicenseState(License.OperationMode.PLATINUM);
        XPackLicenseState enterpriseLicense = makeLicenseState(License.OperationMode.ENTERPRISE);

        private void assertLicenseCheck(
            LicenseAware licenseAware,
            List<DataType> signature,
            boolean allowsBasic,
            boolean allowsPlatinum,
            boolean allowsEnterprise
        ) {
            boolean basic = licenseAware.licenseCheck(basicLicense);
            boolean platinum = licenseAware.licenseCheck(platinumLicense);
            boolean enterprise = licenseAware.licenseCheck(enterpriseLicense);
            assertThat("Basic license should be accepted for " + signature, basic, equalTo(allowsBasic));
            assertThat("Platinum license should be accepted for " + signature, platinum, equalTo(allowsPlatinum));
            assertThat("Enterprise license should be accepted for " + signature, enterprise, equalTo(allowsEnterprise));
        }

        private void assertLicenseCheck(List<DataType> signature, boolean allowed, boolean expected) {
            assertThat("Basic license should " + (expected ? "" : "not ") + "be accepted for " + signature, allowed, equalTo(expected));
        }
    }

    private static XPackLicenseState makeLicenseState(License.OperationMode mode) {
        return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(mode, true, ""));
    }

    /**
     * Asserts the result of a test case matches the expected result and warnings.
     * <p>
     * The {@code result} parameter should be an object as returned by {@link #toJavaObjectUnsignedLongAware}.
     * </p>
     */
    @SuppressWarnings("unchecked")
    protected final void assertTestCaseResultAndWarnings(Object result) {
        if (result instanceof Iterable<?>) {
            var collectionResult = (Iterable<Object>) result;
            assertThat(collectionResult, not(hasItem(Double.NaN)));
            assertThat(collectionResult, not(hasItem(Double.POSITIVE_INFINITY)));
            assertThat(collectionResult, not(hasItem(Double.NEGATIVE_INFINITY)));
        }

        assert testCase.getMatcher().matches(Double.NaN) == false;
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

    protected final void assertTypeResolutionFailure(Expression expression) {
        assertTrue("expected unresolved", expression.typeResolved().unresolved());
        assertThat(expression.typeResolved().message(), equalTo(testCase.getExpectedTypeError()));
    }

    private static Class<?> classGeneratingSignatures = null;
    /**
     * Unique signatures in this test’s parameters.
     */
    private static Map<List<DataType>, DataType> signatures;

    public static Map<List<DataType>, DataType> signatures(Class<?> testClass) {
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
    public static void renderDocs() throws Exception {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        DocsV3Support.renderDocs(functionName(), getTestClass());
    }

    protected static Constructor<?> constructorWithFunctionInfo(Class<?> clazz) {
        for (Constructor<?> ctor : clazz.getConstructors()) {
            FunctionInfo functionInfo = ctor.getAnnotation(FunctionInfo.class);
            if (functionInfo != null) {
                return ctor;
            }
        }
        return null;
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

    static boolean functionRegistered(String name) {
        return functionRegistry.functionExists(name);
    }

    static FunctionDefinition definition(String name) {
        if (functionRegistry.functionExists(name)) {
            return functionRegistry.resolveFunction(name);
        }
        return null;
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

    /**
     * Returns true if the current test case is for an aggregation function.
     * <p>
     *     This method requires reflection, as it’s called from a static context (@AfterClass documentation rendering).
     * </p>
     */
    private static boolean isAggregation() {
        return AbstractAggregationTestCase.class.isAssignableFrom(getTestClass());
    }

    /**
     * Should this particular signature be hidden from the docs even though we test it?
     */
    static boolean shouldHideSignature(List<DataType> argTypes, DataType returnType) {
        for (DataType dt : DataType.UNDER_CONSTRUCTION.keySet()) {
            if (returnType == dt || argTypes.contains(dt)) {
                return true;
            }
        }
        return false;
    }
}
