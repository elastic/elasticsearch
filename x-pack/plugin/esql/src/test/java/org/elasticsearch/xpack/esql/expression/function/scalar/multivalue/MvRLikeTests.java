/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedData.MULTI_ROW_NULL;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TypedData.NULL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class MvRLikeTests extends AbstractScalarFunctionTestCase {
    public MvRLikeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /** A regex has no affix shapes to peel off, so every pattern runs the automaton — no anyOf needed, unlike mv_like. */
    private static Matcher<String> evaluatorMatcher() {
        return startsWith("MvAutomataMatchEvaluator[field=Attribute[channel=0], pattern=digraph Automaton {\n");
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType fieldType : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
            for (DataType patternType : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
                suppliers.add(
                    new TestCaseSupplier(
                        "random " + fieldType.esType() + " / " + patternType.esType(),
                        List.of(fieldType, patternType),
                        () -> {
                            String prefix = randomAlphaOfLength(3);
                            List<BytesRef> values = List.of(
                                new BytesRef(randomAlphaOfLength(4)),
                                new BytesRef(prefix + randomAlphaOfLength(4))
                            );
                            return testCase(values, fieldType, prefix + ".*", patternType, true);
                        }
                    )
                );
            }
        }

        // Any-value reduction: which value matches must not matter.
        addCase(suppliers, "match on the only value", List.of("anna"), "ann.*", true);
        addCase(suppliers, "no match on the only value", List.of("bob"), "ann.*", false);
        addCase(suppliers, "match on the first of three", List.of("anna", "bob", "carl"), "ann.*", true);
        addCase(suppliers, "match on the middle of three", List.of("bob", "anna", "carl"), "ann.*", true);
        addCase(suppliers, "match on the last of three", List.of("bob", "carl", "anna"), "ann.*", true);
        addCase(suppliers, "no match on any of three", List.of("bob", "carl", "dave"), "ann.*", false);
        addCase(suppliers, "duplicates that match", List.of("anna", "anna"), "ann.*", true);

        // RLIKE anchors the whole value — a pattern matching a substring does not match the value.
        addCase(suppliers, "pattern must match the whole value", List.of("annabel"), "anna", false);
        addCase(suppliers, "whole-value match", List.of("anna"), "anna", true);

        // Regex axis.
        addCase(suppliers, "character class", List.of("bob"), "[abc]ob", true);
        addCase(suppliers, "character class does not match", List.of("zob"), "[abc]ob", false);
        addCase(suppliers, "alternation picks the second branch", List.of("carl"), "bob|carl", true);
        addCase(suppliers, "quantifier", List.of("aaa"), "a+", true);
        addCase(suppliers, ". matches exactly one character", List.of("ab"), "a.", true);
        addCase(suppliers, ". does not match two characters", List.of("abc"), "a.", false);
        addCase(suppliers, ".* matches every value", List.of("anything"), ".*", true);
        addCase(suppliers, ".* matches the empty string", List.of(""), ".*", true);
        // The empty pattern: RegExp("") accepts exactly the empty string, so mv_rlike matches "" and only "". Unlike
        // mv_like's empty wildcard, this pushes to Lucene (a RegexpQuery("") matches the empty-string term), so pushed
        // and evaluated agree — the one pattern where the two functions take different pushdown decisions.
        addCase(suppliers, "empty pattern matches only the empty string", List.of(""), "", true);
        addCase(suppliers, "empty pattern does not match a non-empty value", List.of("a"), "", false);
        addCase(suppliers, "escaped dot matches a literal dot", List.of("a.b"), "a\\.b", true);
        addCase(suppliers, "escaped dot does not match an arbitrary character", List.of("axb"), "a\\.b", false);
        addCase(suppliers, "pattern is case sensitive", List.of("Anna"), "ann.*", false);
        addCase(suppliers, "multi-byte value", List.of("éclair"), "é.*", true);

        return parameterSuppliersFromTypedData(anyNullIsNull(suppliers));
    }

    /** Field-only null wrap — see {@link MvLikeTests} for why the base helper cannot be used and why only the field is nulled. */
    private static List<TestCaseSupplier> anyNullIsNull(List<TestCaseSupplier> testCaseSuppliers) {
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers);
        Set<List<DataType>> uniqueSignatures = new HashSet<>();
        for (TestCaseSupplier original : testCaseSuppliers) {
            boolean firstTimeSeenSignature = uniqueSignatures.add(original.types());
            int nullPosition = 0; // the field only; a null pattern is an error, covered in the error and optimizer tests

            suppliers.add(new TestCaseSupplier("G1: " + original.name() + " null field", original.types(), () -> {
                TestCaseSupplier.TestCase originalTestCase = original.get();
                List<TestCaseSupplier.TypedData> typeDataWithNull = new ArrayList<>(originalTestCase.getData());
                var data = typeDataWithNull.get(nullPosition);
                typeDataWithNull.set(nullPosition, data.withData(data.isMultiRow() ? Collections.singletonList(null) : null));
                return new TestCaseSupplier.TestCase(typeDataWithNull, originalTestCase.evaluatorToString(), DataType.BOOLEAN, is(false));
            }));

            if (firstTimeSeenSignature) {
                var typesWithNull = new ArrayList<>(original.types());
                typesWithNull.set(nullPosition, DataType.NULL);
                if (uniqueSignatures.add(typesWithNull)) {
                    suppliers.add(
                        new TestCaseSupplier(
                            "G2: " + typesWithNull.stream().map(Objects::toString).collect(Collectors.joining(" ")) + " null field",
                            typesWithNull,
                            () -> {
                                TestCaseSupplier.TestCase originalTestCase = original.get();
                                var typeDataWithNull = new ArrayList<>(originalTestCase.getData());
                                typeDataWithNull.set(nullPosition, typeDataWithNull.get(nullPosition).isMultiRow() ? MULTI_ROW_NULL : NULL);
                                return new TestCaseSupplier.TestCase(typeDataWithNull, "ConstantFalse", DataType.BOOLEAN, is(false));
                            }
                        )
                    );
                }
            }
        }
        return suppliers;
    }

    private static void addCase(List<TestCaseSupplier> suppliers, String name, List<String> values, String pattern, boolean expected) {
        suppliers.add(
            new TestCaseSupplier(
                name,
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> testCase(
                    values.stream().map(BytesRef::new).collect(Collectors.toList()),
                    DataType.KEYWORD,
                    pattern,
                    DataType.KEYWORD,
                    expected
                )
            )
        );
    }

    private static TestCaseSupplier.TestCase testCase(
        List<BytesRef> values,
        DataType fieldType,
        String pattern,
        DataType patternType,
        boolean expected
    ) {
        return new TestCaseSupplier.TestCase(
            List.of(
                new TestCaseSupplier.TypedData(values, fieldType, "field"),
                new TestCaseSupplier.TypedData(new BytesRef(pattern), patternType, "pattern").forceLiteral()
            ),
            evaluatorMatcher(),
            DataType.BOOLEAN,
            equalTo(expected)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvRLike(source, args.get(0), args.get(1));
    }

    // mv_rlike never returns null: a null or empty field has no value to match, so an all-null position is false.
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(false);
    }
}
