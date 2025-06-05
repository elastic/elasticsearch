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
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
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
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.mapParam;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.param;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.paramWithoutAnnotation;
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
        entry("is_not_null", IsNotNull.class),
        // Match operator is both a function and an operator
        entry("match_operator", MatchOperator.class)
    );

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
                 * adding support for them to functions. That's *why* they are
                 * under construction.
                 */
                return false;
            }
            if (t.isCounter()) {
                /*
                 * For now, we're assuming no functions take counters
                 * as parameters. That's not true - some do. But we'll
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
        if (randomBoolean()) {
            return expression;
        }

        return serializeDeserializeExpression(expression);
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
        assertSerialization(buildFieldExpression(testCase), testCase.getConfiguration());
    }

    /**
     * This test is meant to validate that the params annotations for the function being tested align with the supported types the
     * test framework has detected.
     */
    @AfterClass
    public static void testFunctionInfo() {
        Logger log = LogManager.getLogger(getTestClass());
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
        for (Map.Entry<List<DataType>, DataType> entry : signatures().entrySet()) {
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

    @AfterClass
    public static void renderSignature() throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        String name = functionName();
        String rendered = buildSignatureSvg(name);
        if (rendered == null) {
            LogManager.getLogger(getTestClass()).info("Skipping rendering signature because the function isn't registered");
        } else {
            LogManager.getLogger(getTestClass()).info("Writing function signature");
            writeToTempDir("signature", name, "svg", rendered);
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
        String searchOperator = searchOperator(name);
        if (searchOperator != null) {
            return RailRoadDiagram.searchOperator(searchOperator);
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
        renderDocs(functionName());
    }

    protected static void renderDocs(String name) throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        if (binaryOperator(name) != null || unaryOperator(name) != null || searchOperator(name) != null || likeOrInOperator(name)) {
            renderDocsForOperators(name);
            return;
        }
        FunctionDefinition definition = definition(name);
        if (definition != null) {
            EsqlFunctionRegistry.FunctionDescription description = EsqlFunctionRegistry.description(definition);
            if (name.equals("case")) {
                /*
                 * Hack the description, so we render a proper one for case.
                 */
                // TODO build the description properly *somehow*
                EsqlFunctionRegistry.ArgSignature trueValue = description.args().get(1);
                EsqlFunctionRegistry.ArgSignature falseValue = new EsqlFunctionRegistry.ArgSignature(
                    "elseValue",
                    trueValue.type(),
                    "The value that's returned when no condition evaluates to `true`.",
                    true
                );
                description = new EsqlFunctionRegistry.FunctionDescription(
                    description.name(),
                    List.of(description.args().get(0), trueValue, falseValue),
                    description.returnType(),
                    description.description(),
                    description.variadic(),
                    description.isAggregation()
                );
            }
            renderTypes(name, description.args());
            renderParametersList(name, description.argNames(), description.argDescriptions());
            FunctionInfo info = EsqlFunctionRegistry.functionInfo(definition);
            renderDescription(name, description.description(), info.detailedDescription(), info.note());
            Optional<EsqlFunctionRegistry.ArgSignature> mapArgSignature = description.args()
                .stream()
                .filter(EsqlFunctionRegistry.ArgSignature::mapArg)
                .findFirst();
            boolean hasFunctionOptions = mapArgSignature.isPresent();
            if (hasFunctionOptions) {
                renderFunctionNamedParams(name, (EsqlFunctionRegistry.MapArgSignature) mapArgSignature.get());
            }
            boolean hasExamples = renderExamples(name, info);
            boolean hasAppendix = renderAppendix(name, info.appendix());
            renderFullLayout(name, info.preview(), hasExamples, hasAppendix, hasFunctionOptions);
            renderKibanaInlineDocs(name, info);
            renderKibanaFunctionDefinition(name, info, description.args(), description.variadic());
            return;
        }
        LogManager.getLogger(getTestClass()).info("Skipping rendering types because the function '" + name + "' isn't registered");
    }

    private static final String DOCS_WARNING =
        "// This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.\n\n";

    private static final String PREVIEW_CALLOUT =
        "\npreview::[\"Do not use on production environments. This functionality is in technical preview and "
            + "may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview "
            + "are not subject to the support SLA of official GA features.\"]\n";

    private static void renderTypes(String name, List<EsqlFunctionRegistry.ArgSignature> args) throws IOException {
        StringBuilder header = new StringBuilder();
        List<String> argNames = args.stream().map(EsqlFunctionRegistry.ArgSignature::name).toList();
        for (String arg : argNames) {
            header.append(arg).append(" | ");
        }
        header.append("result");

        List<String> table = new ArrayList<>();
        for (Map.Entry<List<DataType>, DataType> sig : signatures().entrySet()) { // TODO flip to using sortedSignatures
            if (shouldHideSignature(sig.getKey(), sig.getValue())) {
                continue;
            }
            if (sig.getKey().size() > argNames.size()) { // skip variadic [test] cases (but not those with optional parameters)
                continue;
            }
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < sig.getKey().size(); i++) {
                DataType argType = sig.getKey().get(i);
                EsqlFunctionRegistry.ArgSignature argSignature = args.get(i);
                if (argSignature.mapArg()) {
                    b.append("named parameters");
                } else {
                    b.append(argType.esNameIfPossible());
                }
                b.append(" | ");
            }
            b.append("| ".repeat(argNames.size() - sig.getKey().size()));
            b.append(sig.getValue().esNameIfPossible());
            table.add(b.toString());
        }
        Collections.sort(table);
        if (table.isEmpty()) {
            table.add(signatures.values().iterator().next().esNameIfPossible());
        }

        String rendered = DOCS_WARNING + """
            *Supported types*

            [%header.monospaced.styled,format=dsv,separator=|]
            |===
            """ + header + "\n" + table.stream().collect(Collectors.joining("\n")) + "\n|===\n";
        LogManager.getLogger(getTestClass()).info("Writing function types for [{}]:\n{}", name, rendered);
        writeToTempDir("types", name, "asciidoc", rendered);
    }

    private static void renderParametersList(String name, List<String> argNames, List<String> argDescriptions) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append(DOCS_WARNING);
        builder.append("*Parameters*\n");
        for (int a = 0; a < argNames.size(); a++) {
            builder.append("\n`").append(argNames.get(a)).append("`::\n").append(argDescriptions.get(a)).append('\n');
        }
        String rendered = builder.toString();
        LogManager.getLogger(getTestClass()).info("Writing parameters for [{}]:\n{}", name, rendered);
        writeToTempDir("parameters", name, "asciidoc", rendered);
    }

    private static void renderDescription(String name, String description, String detailedDescription, String note) throws IOException {
        String rendered = DOCS_WARNING + """
            *Description*

            """ + description + "\n";

        if (Strings.isNullOrEmpty(detailedDescription) == false) {
            rendered += "\n" + detailedDescription + "\n";
        }

        if (Strings.isNullOrEmpty(note) == false) {
            rendered += "\nNOTE: " + note + "\n";
        }
        LogManager.getLogger(getTestClass()).info("Writing description for [{}]:\n{}", name, rendered);
        writeToTempDir("description", name, "asciidoc", rendered);
    }

    private static boolean renderExamples(String name, FunctionInfo info) throws IOException {
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
        LogManager.getLogger(getTestClass()).info("Writing examples for [{}]:\n{}", name, rendered);
        writeToTempDir("examples", name, "asciidoc", rendered);
        return true;
    }

    private static boolean renderAppendix(String name, String appendix) throws IOException {
        if (appendix.isEmpty()) {
            return false;
        }

        String rendered = DOCS_WARNING + appendix + "\n";

        LogManager.getLogger(getTestClass()).info("Writing appendix for [{}]:\n{}", name, rendered);
        writeToTempDir("appendix", name, "asciidoc", rendered);
        return true;
    }

    private static void renderFunctionNamedParams(String name, EsqlFunctionRegistry.MapArgSignature mapArgSignature) throws IOException {
        String header = "name | types | description";

        List<String> table = new ArrayList<>();
        for (Map.Entry<String, EsqlFunctionRegistry.MapEntryArgSignature> argSignatureEntry : mapArgSignature.mapParams().entrySet()) {
            StringBuilder builder = new StringBuilder();
            EsqlFunctionRegistry.MapEntryArgSignature arg = argSignatureEntry.getValue();
            builder.append(arg.name()).append(" | ").append(arg.type()).append(" | ").append(arg.description());
            table.add(builder.toString());
        }

        String rendered = DOCS_WARNING + """
            *Supported function named parameters*

            [%header.monospaced.styled,format=dsv,separator=|]
            |===
            """ + header + "\n" + table.stream().collect(Collectors.joining("\n")) + "\n|===\n";
        LogManager.getLogger(getTestClass()).info("Writing function named parameters for [{}]:\n{}", functionName(), rendered);
        writeToTempDir("functionNamedParams", name, "asciidoc", rendered);
    }

    private static void renderFullLayout(String name, boolean preview, boolean hasExamples, boolean hasAppendix, boolean hasFunctionOptions)
        throws IOException {
        String rendered = DOCS_WARNING + """
            [discrete]
            [[esql-$NAME$]]
            === `$UPPER_NAME$`
            $PREVIEW_CALLOUT$
            *Syntax*

            [.text-center]
            image::esql/functions/signature/$NAME$.svg[Embedded,opts=inline]

            include::../parameters/$NAME$.asciidoc[]
            include::../description/$NAME$.asciidoc[]
            include::../types/$NAME$.asciidoc[]
            """.replace("$NAME$", name)
            .replace("$UPPER_NAME$", name.toUpperCase(Locale.ROOT))
            .replace("$PREVIEW_CALLOUT$", preview ? PREVIEW_CALLOUT : "");
        if (hasFunctionOptions) {
            rendered += "include::../functionNamedParams/" + name + ".asciidoc[]\n";
        }
        if (hasExamples) {
            rendered += "include::../examples/" + name + ".asciidoc[]\n";
        }
        if (hasAppendix) {
            rendered += "include::../appendix/" + name + ".asciidoc[]\n";
        }
        LogManager.getLogger(getTestClass()).info("Writing layout for [{}]:\n{}", name, rendered);
        writeToTempDir("layout", name, "asciidoc", rendered);
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

    private static void renderDocsForOperators(String name) throws IOException {
        Constructor<?> ctor = constructorWithFunctionInfo(OPERATORS.get(name));
        assert ctor != null;
        FunctionInfo functionInfo = ctor.getAnnotation(FunctionInfo.class);
        assert functionInfo != null;
        renderDocsForOperators(name, ctor, functionInfo);
    }

    protected static void renderDocsForOperators(String name, Constructor<?> ctor, FunctionInfo functionInfo) throws IOException {
        renderKibanaInlineDocs(name, functionInfo);

        var params = ctor.getParameters();

        List<EsqlFunctionRegistry.ArgSignature> args = new ArrayList<>(params.length);
        for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
            if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
                MapParam mapParamInfo = params[i].getAnnotation(MapParam.class);
                if (mapParamInfo != null) {
                    args.add(mapParam(mapParamInfo));
                } else {
                    Param paramInfo = params[i].getAnnotation(Param.class);
                    args.add(paramInfo != null ? param(paramInfo) : paramWithoutAnnotation(params[i].getName()));
                }
            }
        }
        renderKibanaFunctionDefinition(name, functionInfo, args, likeOrInOperator(name));
        renderTypes(name, args);
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
        LogManager.getLogger(getTestClass()).info("Writing kibana inline docs for [{}]:\n{}", name, rendered);
        writeToTempDir("kibana/docs", name, "md", rendered);
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
        if (false == info.operator().isEmpty()) {
            builder.field("type", "operator");
            builder.field("operator", info.operator());
            assertThat(isAggregation(), equalTo(false));
        } else {
            builder.field("type", isAggregation() ? "agg" : "eval");
        }
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
            builder.field("returnType", signatures().values().iterator().next().esNameIfPossible());
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
                if (shouldHideSignature(sig.getKey(), sig.getValue())) {
                    continue;
                }
                builder.startObject();
                builder.startArray("params");
                for (int i = 0; i < sig.getKey().size(); i++) {
                    EsqlFunctionRegistry.ArgSignature arg = args.get(i);
                    builder.startObject();
                    builder.field("name", arg.name());
                    if (arg.mapArg()) {
                        builder.field("type", "function_named_parameters");
                        builder.field(
                            "mapParams",
                            arg.mapParams()
                                .values()
                                .stream()
                                .map(mapArgSignature -> "{" + mapArgSignature + "}")
                                .collect(Collectors.joining(", "))
                        );
                    } else {
                        builder.field("type", sig.getKey().get(i).esNameIfPossible());
                    }
                    builder.field("optional", arg.optional());
                    builder.field("description", arg.description());
                    builder.endObject();
                }
                builder.endArray();
                builder.field("variadic", variadic);
                builder.field("returnType", sig.getValue().esNameIfPossible());
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
        builder.field("preview", info.preview());
        builder.field("snapshot_only", EsqlFunctionRegistry.isSnapshotOnly(name));

        String rendered = Strings.toString(builder.endObject());
        LogManager.getLogger(getTestClass()).info("Writing kibana function definition for [{}]:\n{}", name, rendered);
        writeToTempDir("kibana/definition", name, "json", rendered);
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
                    int c = lhs.getKey().get(i).esNameIfPossible().compareTo(rhs.getKey().get(i).esNameIfPossible());
                    if (c != 0) {
                        return c;
                    }
                }
                return lhs.getValue().esNameIfPossible().compareTo(rhs.getValue().esNameIfPossible());
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
        if (functionRegistry.functionExists(name)) {
            return functionRegistry.resolveFunction(name);
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
     * If this test is a for a search operator return its symbol, otherwise return {@code null}.
     */
    private static String searchOperator(String name) {
        return switch (name) {
            case "match_operator" -> ":";
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
        return switch (name.toLowerCase(Locale.ENGLISH)) {
            case "rlike", "like", "in", "not_rlike", "not_like", "not_in" -> true;
            default -> false;
        };
    }

    /**
     * Write some text to a tempdir so we can copy it to the docs later.
     * <p>
     * We need to write to a tempdir instead of the docs because the tests
     * don't have write permission to the docs.
     * </p>
     */
    private static void writeToTempDir(String subdir, String name, String extension, String str) throws IOException {
        // We have to write to a tempdir because it's all test are allowed to write to. Gradle can move them.
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("functions").resolve(subdir);
        Files.createDirectories(dir);
        Path file = dir.resolve(name + "." + extension);
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

    /**
     * Returns true if the current test case is for an aggregation function.
     * <p>
     *     This method requires reflection, as it's called from a static context (@AfterClass documentation rendering).
     * </p>
     */
    private static boolean isAggregation() {
        return AbstractAggregationTestCase.class.isAssignableFrom(getTestClass());
    }

    /**
     * Should this particular signature be hidden from the docs even though we test it?
     */
    private static boolean shouldHideSignature(List<DataType> argTypes, DataType returnType) {
        for (DataType dt : DataType.UNDER_CONSTRUCTION.keySet()) {
            if (returnType == dt || argTypes.contains(dt)) {
                return true;
            }
        }
        return false;
    }
}
