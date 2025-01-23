/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.stringCases;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("match")
public class MatchTests extends AbstractFunctionTestCase {

    public MatchTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    protected static List<TestCaseSupplier> parametersWithoutFunctionNamedParams() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        addUnsignedLongCases(suppliers);
        addNumericCases(suppliers);
        addNonNumericCases(suppliers);
        addQueryAsStringTestCases(suppliers);
        addStringTestCases(suppliers);

        return suppliers;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addFunctionNamedParams(parametersWithoutFunctionNamedParams()));
    }

    protected boolean hasFunctionNamedParams() {
        return true;
    }

    private static void addNonNumericCases(List<TestCaseSupplier> suppliers) {
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.booleanCases(),
                TestCaseSupplier.booleanCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.ipCases(),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.versionCases(""),
                List.of(),
                false
            )
        );
        // Datetime
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.dateCases(),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.dateNanosCases(),
                List.of(),
                false
            )
        );
    }

    private static void addNumericCases(List<TestCaseSupplier> suppliers) {
        suppliers.addAll(
            TestCaseSupplier.forBinaryComparisonWithWidening(
                new TestCaseSupplier.NumericTypeTestConfigs<>(
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Integer.MIN_VALUE >> 1) - 1,
                        (Integer.MAX_VALUE >> 1) - 1,
                        (l, r) -> true,
                        "EqualsIntsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        (Long.MIN_VALUE >> 1) - 1,
                        (Long.MAX_VALUE >> 1) - 1,
                        (l, r) -> true,
                        "EqualsLongsEvaluator"
                    ),
                    new TestCaseSupplier.NumericTypeTestConfig<>(
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        // NB: this has different behavior than Double::equals
                        (l, r) -> true,
                        "EqualsDoublesEvaluator"
                    )
                ),
                "field",
                "query",
                (lhs, rhs) -> List.of(),
                false
            )
        );
    }

    private static List<TestCaseSupplier> addFunctionNamedParams(Collection<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> result = new ArrayList<>();
        for (TestCaseSupplier supplier : suppliers) {
            List<DataType> dataTypes = new ArrayList<>(supplier.types());
            dataTypes.add(UNSUPPORTED);
            result.add(new TestCaseSupplier(supplier.name() + ", options", dataTypes, () -> {
                List<TestCaseSupplier.TypedData> values = new ArrayList<>(supplier.get().getData());
                values.add(
                    new TestCaseSupplier.TypedData(
                        new MapExpression(
                            Source.EMPTY,
                            List.of(
                                new Literal(Source.EMPTY, "fuzziness", KEYWORD),
                                new Literal(Source.EMPTY, randomAlphaOfLength(10), KEYWORD)
                            )
                        ),
                        UNSUPPORTED,
                        "options"
                    ).forceLiteral()
                );

                return new TestCaseSupplier.TestCase(values, equalTo("MatchEvaluator"), DataType.BOOLEAN, equalTo(true));
            }));
        }
        return result;
    }

    private static void addUnsignedLongCases(List<TestCaseSupplier> suppliers) {
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.longCases(Long.MIN_VALUE, Long.MAX_VALUE, true),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.doubleCases(Double.MIN_VALUE, Double.MAX_VALUE, true),
                List.of(),
                false
            )
        );
    }

    private static void addQueryAsStringTestCases(List<TestCaseSupplier> suppliers) {

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.longCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.doubleCases(Double.MIN_VALUE, Double.MAX_VALUE, true),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );

        // Unsigned Long cases
        // TODO: These should be integrated into the type cross product above, but are currently broken
        // see https://github.com/elastic/elasticsearch/issues/102935
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.booleanCases(),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ipCases(),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.versionCases(""),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );
        // Datetime
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateCases(),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );

        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.dateNanosCases(),
                TestCaseSupplier.stringCases(KEYWORD),
                List.of(),
                false
            )
        );
    }

    private static void addStringTestCases(List<TestCaseSupplier> suppliers) {
        for (DataType fieldType : DataType.stringTypes()) {
            if (DataType.UNDER_CONSTRUCTION.containsKey(fieldType)) {
                continue;
            }
            for (TestCaseSupplier.TypedDataSupplier queryDataSupplier : stringCases(fieldType)) {
                suppliers.add(
                    TestCaseSupplier.testCaseSupplier(
                        queryDataSupplier,
                        new TestCaseSupplier.TypedDataSupplier(fieldType.typeName(), () -> randomAlphaOfLength(10), KEYWORD),
                        (d1, d2) -> equalTo("string"),
                        DataType.BOOLEAN,
                        (o1, o2) -> true
                    )
                );
            }
        }
    }

    public final void testLiteralExpressions() {
        Expression expression = buildLiteralExpression(testCase);
        assertFalse("expected resolved", expression.typeResolved().unresolved());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Match match = new Match(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
        // We need to add the QueryBuilder to the match expression, as it is used to implement equals() and hashCode() and
        // thus test the serialization methods. But we can only do this if the parameters make sense .
        if (args.get(0) instanceof FieldAttribute && args.get(1).foldable()) {
            QueryBuilder queryBuilder = TRANSLATOR_HANDLER.asQuery(match).asBuilder();
            match.replaceQueryBuilder(queryBuilder);
        }
        return match;
    }
}
