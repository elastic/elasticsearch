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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

@FunctionName("match_phrase")
public class MatchPhraseTests extends SingleFieldFullTextFunctionTestCase {

    public MatchPhraseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addFunctionNamedParams(addNullFieldTestCases(testCaseSuppliers()), mapExpressionSupplier()));
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        addStringTestCases(suppliers);
        return addNullFieldTestCases(suppliers);
    }

    private static Supplier<MapExpression> mapExpressionSupplier() {
        return () -> new MapExpression(
            Source.EMPTY,
            List.of(new Literal(Source.EMPTY, "slop", INTEGER), Literal.integer(Source.EMPTY, randomNonNegativeInt()))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MatchPhrase(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    public void testToTextUnionFieldWithLegacyRepresentationAndNonTextBranchIsRuntimeSearch() {
        FieldAttribute field = MatchTests.unionFieldAttribute("field", DataType.TEXT, true, DataType.KEYWORD, DataType.TEXT);
        MatchPhrase match = new MatchPhrase(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the legacy MultiTypeEsField representation with a non-TEXT branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }

    public void testToStringUnionFieldWithLegacyRepresentationAndNonKeywordBranchIsRuntimeSearch() {
        FieldAttribute field = MatchTests.unionFieldAttribute("field", DataType.KEYWORD, true, DataType.TEXT, DataType.KEYWORD);
        MatchPhrase match = new MatchPhrase(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the legacy MultiTypeEsField representation with a non-KEYWORD branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }

    public void testToTextUnionFieldWithCompactRepresentationAndNonTextBranchIsRuntimeSearch() {
        FieldAttribute field = MatchTests.unionFieldAttribute("field", DataType.TEXT, false, DataType.KEYWORD, DataType.TEXT);
        MatchPhrase match = new MatchPhrase(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the compact representation with a non-TEXT branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }

    public void testToStringUnionFieldWithCompactRepresentationAndNonKeywordBranchIsRuntimeSearch() {
        FieldAttribute field = MatchTests.unionFieldAttribute("field", DataType.KEYWORD, false, DataType.TEXT, DataType.KEYWORD);
        MatchPhrase match = new MatchPhrase(Source.EMPTY, field, new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD), null);
        assertTrue(
            "a union field resolved via the compact representation with a non-KEYWORD branch must not be pushed down",
            match.isRuntimeSearch()
        );
    }
}
