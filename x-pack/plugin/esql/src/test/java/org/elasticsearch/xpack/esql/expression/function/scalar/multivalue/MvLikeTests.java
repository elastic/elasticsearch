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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class MvLikeTests extends AbstractScalarFunctionTestCase {
    public MvLikeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * mv_like dispatches on the pattern's shape, so any of four evaluators is a legitimate result: the affix fast paths
     * for {@code literal*}, {@code *literal} and {@code *literal*}, and the automaton for everything else. Pinning only
     * the automaton here would silently forbid the fast paths — the same reason {@code WildcardLikeTests} matches with
     * an {@code anyOf}. The automaton kernel carries the determinized automaton's dot rendering as its {@code @Fixed}
     * pattern, exactly as {@code AutomataMatch} does, so its {@code toString} opens with the dot graph.
     */
    private static Matcher<String> evaluatorMatcher() {
        return anyOf(
            startsWith("MvAutomataMatchEvaluator[field=Attribute[channel=0], pattern=digraph Automaton {\n"),
            startsWith("MvLikeAffixMatchEvaluator[field=Attribute[channel=0], shape=")
        );
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // One randomized supplier per supported field type, so the coverage guard (resolveType == @Param == tests) holds.
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
                            return testCase(values, fieldType, prefix + "*", patternType, true);
                        }
                    )
                );
            }
        }

        // Any-value reduction: which value matches must not matter.
        addCase(suppliers, "match on the only value", List.of("anna"), "ann*", true);
        addCase(suppliers, "no match on the only value", List.of("bob"), "ann*", false);
        addCase(suppliers, "match on the first of three", List.of("anna", "bob", "carl"), "ann*", true);
        addCase(suppliers, "match on the middle of three", List.of("bob", "anna", "carl"), "ann*", true);
        addCase(suppliers, "match on the last of three", List.of("bob", "carl", "anna"), "ann*", true);
        addCase(suppliers, "no match on any of three", List.of("bob", "carl", "dave"), "ann*", false);
        addCase(suppliers, "duplicates that match", List.of("anna", "anna"), "ann*", true);
        addCase(suppliers, "duplicates that do not match", List.of("bob", "bob"), "ann*", false);

        // An empty (zero-value) field cannot be expressed as TypedData here — the harness indexes position 0 — so the
        // null-field arm of the two-valued contract is pinned in string.csv-spec (mvLikeNullField), and the genuinely
        // zero-value arm end to end, where a real index can produce it.
        // What this suite pins is the neighbouring case: a present but empty *string* value is a value, and matches `*`.
        addCase(suppliers, "empty string value against *", List.of(""), "*", true);

        // Pattern axis.
        addCase(suppliers, "* alone matches any value", List.of("anything"), "*", true);
        addCase(suppliers, "? matches exactly one character", List.of("ab"), "a?", true);
        addCase(suppliers, "? does not match two characters", List.of("abc"), "a?", false);
        addCase(suppliers, "empty pattern matches only the empty string", List.of(""), "", true);
        addCase(suppliers, "empty pattern does not match a non-empty value", List.of("a"), "", false);
        addCase(suppliers, "exact pattern with no metacharacters", List.of("anna"), "anna", true);
        addCase(suppliers, "exact pattern does not match a prefix", List.of("annabel"), "anna", false);
        addCase(suppliers, "contains pattern", List.of("xxannaxx"), "*anna*", true);
        addCase(suppliers, "suffix pattern", List.of("bobanna"), "*anna", true);

        // Escaping: the pattern is escaped, the data never is. A literal '*' in the data is matched by '\*', not by '*'
        // meaning "any run" — and a value without the literal is not matched by the escaped pattern.
        addCase(suppliers, "escaped star matches a literal star in data", List.of("a*b"), "a\\*b", true);
        addCase(suppliers, "escaped star does not match an arbitrary value", List.of("axb"), "a\\*b", false);
        addCase(suppliers, "unescaped star matches the literal-star value too", List.of("a*b"), "a*b", true);
        addCase(suppliers, "escaped question mark matches a literal question mark", List.of("a?b"), "a\\?b", true);
        addCase(suppliers, "escaped question mark does not match an arbitrary character", List.of("axb"), "a\\?b", false);

        // Case sensitivity — v1 has no insensitive mode.
        addCase(suppliers, "pattern is case sensitive", List.of("Anna"), "ann*", false);
        addCase(suppliers, "pattern matches with the right case", List.of("Anna"), "Ann*", true);

        // The automaton runs over UTF-8 bytes; '?' must still mean one codepoint, not one byte.
        addCase(suppliers, "? matches a multi-byte codepoint", List.of("é"), "?", true);
        addCase(suppliers, "multi-byte value matched by a prefix pattern", List.of("éclair"), "é*", true);

        return parameterSuppliersFromTypedData(anyNullIsNull(suppliers));
    }

    /**
     * A field-only variant of {@link org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase#anyNullIsNull}:
     * mv_like never returns null, so a null or null-typed <em>field</em> yields {@code false}. Only the field (position 0)
     * is nulled — a null <em>pattern</em> is an author error rejected before it can match (in postOptimizationVerification,
     * or in {@code patternString} when the whole call is constant-folded), so it cannot be a value case here (it is
     * covered as an error in {@code MvLikeErrorTests} and the optimizer tests). Modeled on the same helper in
     * {@code MvIntersectsTests} / {@code MvInRangeTests}.
     */
    private static List<TestCaseSupplier> anyNullIsNull(List<TestCaseSupplier> testCaseSuppliers) {
        List<TestCaseSupplier> suppliers = new ArrayList<>(testCaseSuppliers);
        Set<List<DataType>> uniqueSignatures = new HashSet<>();
        for (TestCaseSupplier original : testCaseSuppliers) {
            boolean firstTimeSeenSignature = uniqueSignatures.add(original.types());
            int nullPosition = 0; // the field only

            // A runtime null field: still the ordinary evaluator over a null block, result false.
            suppliers.add(new TestCaseSupplier("G1: " + original.name() + " null field", original.types(), () -> {
                TestCaseSupplier.TestCase originalTestCase = original.get();
                List<TestCaseSupplier.TypedData> typeDataWithNull = new ArrayList<>(originalTestCase.getData());
                var data = typeDataWithNull.get(nullPosition);
                typeDataWithNull.set(nullPosition, data.withData(data.isMultiRow() ? Collections.singletonList(null) : null));
                return new TestCaseSupplier.TestCase(typeDataWithNull, originalTestCase.evaluatorToString(), DataType.BOOLEAN, is(false));
            }));

            // A null-typed field: the predicate folds to a constant false before any evaluator is built.
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
        return new MvLike(source, args.get(0), args.get(1));
    }

    // mv_like never returns null: a null or empty field has no value to match, so an all-null position is false.
    @Override
    protected Matcher<Object> allNullsMatcher() {
        return equalTo(false);
    }
}
