/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Evaluator tests for the internal {@link HashOffset} sampling offset: every key set maps to an
 * offset in {@code [0, 1)}, null and empty keys still hash, and the mapping is stable.
 */
public class HashOffsetTests extends AbstractScalarFunctionTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = List.of(
            emptyKeys(),
            singleKeyword(),
            singleLong(),
            twoKeys(),
            nullKey(),
            multiValueKey(),
            pinnedKeywordOffset()
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    public HashOffsetTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new HashOffset(source, args);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Matcher<Object> allNullsMatcher() {
        // Null keys hash like any other identity: null rows still produce an offset, never null.
        return (Matcher<Object>) (Matcher<?>) both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0));
    }

    @Override
    public void testFold() {
        // HashOffset never folds: every row must hash in the engine.
    }

    private static TestCaseSupplier emptyKeys() {
        return new TestCaseSupplier(
            "emptyKeys",
            List.of(),
            () -> new TestCaseSupplier.TestCase(
                List.of(),
                "HashOffset[]",
                DataType.DOUBLE,
                both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0))
            )
        );
    }

    private static TestCaseSupplier singleKeyword() {
        return new TestCaseSupplier(
            "singleKeyword",
            List.of(DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef("pod-one"), DataType.KEYWORD, "k")),
                "HashOffset[Attribute[channel=0]]",
                DataType.DOUBLE,
                both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0))
            )
        );
    }

    private static TestCaseSupplier singleLong() {
        return new TestCaseSupplier(
            "singleLong",
            List.of(DataType.LONG),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(42L, DataType.LONG, "k")),
                "HashOffset[Attribute[channel=0]]",
                DataType.DOUBLE,
                both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0))
            )
        );
    }

    private static TestCaseSupplier twoKeys() {
        return new TestCaseSupplier(
            "twoKeys",
            List.of(DataType.KEYWORD, DataType.LONG),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef("prod"), DataType.KEYWORD, "k1"),
                    new TestCaseSupplier.TypedData(7L, DataType.LONG, "k2")
                ),
                "HashOffset[Attribute[channel=0], Attribute[channel=1]]",
                DataType.DOUBLE,
                both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0))
            )
        );
    }

    private static TestCaseSupplier nullKey() {
        return new TestCaseSupplier(
            "nullKey",
            List.of(DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(null, DataType.KEYWORD, "k")),
                "HashOffset[Attribute[channel=0]]",
                DataType.DOUBLE,
                both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0))
            )
        );
    }

    private static TestCaseSupplier multiValueKey() {
        return new TestCaseSupplier(
            "multiValueKey",
            List.of(DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(new BytesRef("eu"), new BytesRef("us")), DataType.KEYWORD, "k")),
                "HashOffset[Attribute[channel=0]]",
                DataType.DOUBLE,
                both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0))
            )
        );
    }

    /**
     * Pins the stable mapping: the same identity hashes to the same offset on every JVM,
     * unlike the previous per-JVM hash seed.
     */
    private static TestCaseSupplier pinnedKeywordOffset() {
        return new TestCaseSupplier(
            "pinnedKeywordOffset",
            List.of(DataType.KEYWORD),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(new BytesRef("stable-identity"), DataType.KEYWORD, "k")),
                "HashOffset[Attribute[channel=0]]",
                DataType.DOUBLE,
                closeTo(0.4461064581531437, 1e-15)
            )
        );
    }
}
