/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;

@FunctionName("match")
public class MatchTests extends SingleFieldFullTextFunctionTestCase {

    public MatchTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static List<TestCaseSupplier.TypedDataSupplier> forceLiteral(List<TestCaseSupplier.TypedDataSupplier> suppliers) {
        return suppliers.stream().map(s -> new TestCaseSupplier.TypedDataSupplier(s.name(), s.supplier(), s.type(), true)).toList();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addNullFieldTestCases(addFunctionNamedParams(testCaseSuppliers(), mapExpressionSupplier())));
    }

    private static Supplier<MapExpression> mapExpressionSupplier() {
        return () -> new MapExpression(
            Source.EMPTY,
            List.of(Literal.keyword(Source.EMPTY, "max_expansions"), Literal.integer(Source.EMPTY, randomIntBetween(1, 50)))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Match(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    /**
     * Builds a {@link FieldAttribute} backed by a union-typed ({@code UnionTypeEsField}) field with the given
     * per-branch source types, resolved to {@code targetType} - simulating a genuinely type-conflicted field
     * (e.g. mapped {@code keyword} in one index, {@code text} in another) after analysis, where
     * {@code ResolveUnionTypes} has already replaced the original {@code TO_TEXT}/{@code TO_STRING} conversion
     * with a synthetic {@link FieldAttribute} carrying the per-branch conversion knowledge (see
     * {@code Analyzer.ResolveUnionTypes}, "Replace the entire convert function with a new FieldAttribute").
     * {@code legacy} selects between the two {@code UnionTypeEsField} representations: {@code true} for the
     * pre-{@code compact_multi_type_es_field} {@link MultiTypeEsField} (keyed by index name - what a
     * cross-cluster search against an older remote cluster still produces), {@code false} for the modern
     * {@link CompactMultiTypeEsField} (keyed by source type).
     */
    static FieldAttribute unionFieldAttribute(String name, DataType targetType, boolean legacy, DataType... branchSourceTypes) {
        Configuration config = randomConfiguration();
        Map<DataType, Expression> byType = new HashMap<>();
        Map<String, Expression> byIndex = new HashMap<>();
        int i = 0;
        for (DataType sourceType : branchSourceTypes) {
            FieldAttribute source = new FieldAttribute(
                Source.EMPTY,
                name,
                new EsField(name, sourceType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            );
            Expression convert = targetType == DataType.KEYWORD
                ? new ToString(Source.EMPTY, source, config)
                : new ToText(Source.EMPTY, source);
            if (legacy) {
                byIndex.put("idx" + i++, convert);
            } else {
                byType.put(sourceType, convert);
            }
        }
        EsField esField = legacy
            ? new MultiTypeEsField(name, targetType, true, byIndex, EsField.TimeSeriesFieldType.NONE, null)
            : new CompactMultiTypeEsField(name, targetType, true, byType, EsField.TimeSeriesFieldType.NONE, null);
        return new FieldAttribute(Source.EMPTY, name, esField);
    }

    public void testToTextUnionFieldWithLegacyRepresentationAndNonTextBranchIsRuntimeSearch() {
        FieldAttribute field = unionFieldAttribute("field", DataType.TEXT, true, DataType.KEYWORD, DataType.TEXT);
        Match match = new Match(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the legacy MultiTypeEsField representation with a non-TEXT branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }

    public void testToStringUnionFieldWithLegacyRepresentationAndNonKeywordBranchIsRuntimeSearch() {
        FieldAttribute field = unionFieldAttribute("field", DataType.KEYWORD, true, DataType.TEXT, DataType.KEYWORD);
        Match match = new Match(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the legacy MultiTypeEsField representation with a non-KEYWORD branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }

    public void testToTextUnionFieldWithCompactRepresentationAndNonTextBranchIsRuntimeSearch() {
        FieldAttribute field = unionFieldAttribute("field", DataType.TEXT, false, DataType.KEYWORD, DataType.TEXT);
        Match match = new Match(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the compact representation with a non-TEXT branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }

    public void testToStringUnionFieldWithCompactRepresentationAndNonKeywordBranchIsRuntimeSearch() {
        FieldAttribute field = unionFieldAttribute("field", DataType.KEYWORD, false, DataType.TEXT, DataType.KEYWORD);
        Match match = new Match(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the compact representation with a non-KEYWORD branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }

    protected static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        addUnsignedLongCases(suppliers);
        addNumericCases(suppliers);
        addNonNumericCases(suppliers);
        addQueryAsStringTestCases(suppliers);
        addStringTestCases(suppliers);
        addNullFieldTestCases(suppliers);
        return suppliers;
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
                forceLiteral(TestCaseSupplier.booleanCases()),
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
                forceLiteral(TestCaseSupplier.ipCases()),
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
                forceLiteral(TestCaseSupplier.versionCases("")),
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
                forceLiteral(TestCaseSupplier.dateCases()),
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
                forceLiteral(TestCaseSupplier.dateNanosCases()),
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
                false,
                true
            )
        );
    }

    private static void addUnsignedLongCases(List<TestCaseSupplier> suppliers) {
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                null,
                "field",
                "query",
                Object::equals,
                DataType.BOOLEAN,
                TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true),
                forceLiteral(TestCaseSupplier.ulongCases(BigInteger.ZERO, NumericUtils.UNSIGNED_LONG_MAX, true)),
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
                forceLiteral(TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true)),
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
                forceLiteral(TestCaseSupplier.longCases(Long.MIN_VALUE, Long.MAX_VALUE, true)),
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
                forceLiteral(TestCaseSupplier.doubleCases(Double.MIN_VALUE, Double.MAX_VALUE, true)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
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
                forceLiteral(TestCaseSupplier.stringCases(DataType.KEYWORD)),
                List.of(),
                false
            )
        );
    }
}
