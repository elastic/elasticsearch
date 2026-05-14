/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the evaluateFilter path in {@link ParquetPushedExpressions}, which evaluates
 * pushed filter expressions against decoded block values to produce a row-survivor mask.
 */
public class ParquetPushedExpressionsEvaluatorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    // ---- Test 1: All 6 comparison ops with LongBlock ----

    public void testComparisonOpsWithLongBlock() {
        // Block with values [10, 20, 30, 40, 50]
        long[] values = { 10L, 20L, 30L, 40L, 50L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        int rowCount = 5;
        WordMask reusable = new WordMask();

        // Equals: x == 30 -> positions [2]
        assertSurvivors(
            new ParquetPushedExpressions(List.of(new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(30L, DataType.LONG), null))),
            blocks,
            rowCount,
            reusable,
            new int[] { 2 }
        );

        // NotEquals: x != 30 -> positions [0, 1, 3, 4]
        assertSurvivors(
            new ParquetPushedExpressions(List.of(new NotEquals(Source.EMPTY, attr("x", DataType.LONG), lit(30L, DataType.LONG), null))),
            blocks,
            rowCount,
            reusable,
            new int[] { 0, 1, 3, 4 }
        );

        // GreaterThan: x > 30 -> positions [3, 4]
        assertSurvivors(
            new ParquetPushedExpressions(List.of(new GreaterThan(Source.EMPTY, attr("x", DataType.LONG), lit(30L, DataType.LONG), null))),
            blocks,
            rowCount,
            reusable,
            new int[] { 3, 4 }
        );

        // GreaterThanOrEqual: x >= 30 -> positions [2, 3, 4]
        assertSurvivors(
            new ParquetPushedExpressions(
                List.of(new GreaterThanOrEqual(Source.EMPTY, attr("x", DataType.LONG), lit(30L, DataType.LONG), null))
            ),
            blocks,
            rowCount,
            reusable,
            new int[] { 2, 3, 4 }
        );

        // LessThan: x < 30 -> positions [0, 1]
        assertSurvivors(
            new ParquetPushedExpressions(List.of(new LessThan(Source.EMPTY, attr("x", DataType.LONG), lit(30L, DataType.LONG), null))),
            blocks,
            rowCount,
            reusable,
            new int[] { 0, 1 }
        );

        // LessThanOrEqual: x <= 30 -> positions [0, 1, 2]
        assertSurvivors(
            new ParquetPushedExpressions(
                List.of(new LessThanOrEqual(Source.EMPTY, attr("x", DataType.LONG), lit(30L, DataType.LONG), null))
            ),
            blocks,
            rowCount,
            reusable,
            new int[] { 0, 1, 2 }
        );
    }

    // ---- Test 2: Equals across all 5 block types ----

    public void testEqualsWithIntBlock() {
        int[] values = { 1, 2, 3, 4 };
        Block block = blockFactory.newIntArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("i", block);
        WordMask reusable = new WordMask();

        assertSurvivors(
            new ParquetPushedExpressions(List.of(new Equals(Source.EMPTY, attr("i", DataType.INTEGER), lit(3, DataType.INTEGER), null))),
            blocks,
            4,
            reusable,
            new int[] { 2 }
        );
    }

    public void testEqualsWithLongBlock() {
        long[] values = { 100L, 200L, 300L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("l", block);
        WordMask reusable = new WordMask();

        assertSurvivors(
            new ParquetPushedExpressions(List.of(new Equals(Source.EMPTY, attr("l", DataType.LONG), lit(200L, DataType.LONG), null))),
            blocks,
            3,
            reusable,
            new int[] { 1 }
        );
    }

    public void testEqualsWithDoubleBlock() {
        double[] values = { 1.1, 2.2, 3.3 };
        Block block = blockFactory.newDoubleArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("d", block);
        WordMask reusable = new WordMask();

        assertSurvivors(
            new ParquetPushedExpressions(List.of(new Equals(Source.EMPTY, attr("d", DataType.DOUBLE), lit(2.2, DataType.DOUBLE), null))),
            blocks,
            3,
            reusable,
            new int[] { 1 }
        );
    }

    public void testEqualsWithBytesRefBlock() {
        try (var builder = blockFactory.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendBytesRef(new BytesRef("bar"));
            builder.appendBytesRef(new BytesRef("baz"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            assertSurvivors(
                new ParquetPushedExpressions(
                    List.of(new Equals(Source.EMPTY, attr("s", DataType.KEYWORD), lit(new BytesRef("bar"), DataType.KEYWORD), null))
                ),
                blocks,
                3,
                reusable,
                new int[] { 1 }
            );
        }
    }

    public void testEqualsWithBooleanBlock() {
        boolean[] values = { true, false, true, false };
        Block block = blockFactory.newBooleanArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("b", block);
        WordMask reusable = new WordMask();

        assertSurvivors(
            new ParquetPushedExpressions(
                List.of(new Equals(Source.EMPTY, attr("b", DataType.BOOLEAN), lit(false, DataType.BOOLEAN), null))
            ),
            blocks,
            4,
            reusable,
            new int[] { 1, 3 }
        );
    }

    // ---- Test 3: IsNull / IsNotNull ----

    public void testIsNullAndIsNotNull() {
        // Block: [10, null, 30, null, 50]
        try (var builder = blockFactory.newLongBlockBuilder(5)) {
            builder.appendLong(10L);
            builder.appendNull();
            builder.appendLong(30L);
            builder.appendNull();
            builder.appendLong(50L);
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            int rowCount = 5;

            // IS NULL -> positions [1, 3]
            assertSurvivors(
                new ParquetPushedExpressions(List.of(new IsNull(Source.EMPTY, attr("x", DataType.LONG)))),
                blocks,
                rowCount,
                reusable,
                new int[] { 1, 3 }
            );

            // IS NOT NULL -> positions [0, 2, 4]
            assertSurvivors(
                new ParquetPushedExpressions(List.of(new IsNotNull(Source.EMPTY, attr("x", DataType.LONG)))),
                blocks,
                rowCount,
                reusable,
                new int[] { 0, 2, 4 }
            );
        }
    }

    // ---- Test 4: In expression ----

    public void testInExpression() {
        // Block: [1, 2, 3, 4, 5]
        long[] values = { 1L, 2L, 3L, 4L, 5L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // IN(2, 4) -> positions [1, 3]
        Expression inExpr = new In(Source.EMPTY, attr("x", DataType.LONG), List.of(lit(2L, DataType.LONG), lit(4L, DataType.LONG)));
        assertSurvivors(new ParquetPushedExpressions(List.of(inExpr)), blocks, 5, reusable, new int[] { 1, 3 });
    }

    public void testInExpressionWithNonFoldableItemsReturnsNull() {
        // When all items are non-foldable (no literal values), evaluateIn returns null
        // meaning all rows survive (conservative). We simulate this by using a Literal with null value.
        long[] values = { 1L, 2L, 3L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // IN with null literal values -> all null foldables -> empty values -> returns null -> all survive
        Expression inExpr = new In(Source.EMPTY, attr("x", DataType.LONG), List.of(lit(null, DataType.LONG), lit(null, DataType.LONG)));
        WordMask result = new ParquetPushedExpressions(List.of(inExpr)).evaluateFilter(blocks, 3, reusable);
        // null return means all rows survive
        assertNull(result);
    }

    // ---- Test 5: Range expression ----

    public void testRangeClosedInclusive() {
        // Block: [5, 10, 15, 20, 25, 30, 35]
        long[] values = { 5L, 10L, 15L, 20L, 25L, 30L, 35L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // 10 <= x <= 30 -> positions [1, 2, 3, 4, 5]
        Expression range = new Range(
            Source.EMPTY,
            attr("x", DataType.LONG),
            lit(10L, DataType.LONG),
            true,
            lit(30L, DataType.LONG),
            true,
            ZoneOffset.UTC
        );
        assertSurvivors(new ParquetPushedExpressions(List.of(range)), blocks, 7, reusable, new int[] { 1, 2, 3, 4, 5 });
    }

    public void testRangeExclusiveBounds() {
        // Block: [5, 10, 15, 20, 25, 30, 35]
        long[] values = { 5L, 10L, 15L, 20L, 25L, 30L, 35L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // 10 < x < 30 -> positions [2, 3, 4]
        Expression range = new Range(
            Source.EMPTY,
            attr("x", DataType.LONG),
            lit(10L, DataType.LONG),
            false,
            lit(30L, DataType.LONG),
            false,
            ZoneOffset.UTC
        );
        assertSurvivors(new ParquetPushedExpressions(List.of(range)), blocks, 7, reusable, new int[] { 2, 3, 4 });
    }

    public void testRangeLowerOnly() {
        // Block: [5, 10, 15, 20, 25]
        long[] values = { 5L, 10L, 15L, 20L, 25L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // x >= 15 (no upper bound) -> the Range needs both bounds for evaluateRange to produce a result.
        // Range with null upper literal: evaluateRange checks both and returns null if both are null.
        // With lower=15 inclusive and upper=null, the code returns null since upperBound will be null from buildPredicate.
        // However, evaluateRange returns null only when BOTH lower and upper are null.
        // With lower=15 and upper=null: lower is non-null, upper is null, so hasHi=false, which means no upper check.
        // -> positions [2, 3, 4]
        Expression range = new Range(
            Source.EMPTY,
            attr("x", DataType.LONG),
            lit(15L, DataType.LONG),
            true,
            lit(null, DataType.LONG),
            true,
            ZoneOffset.UTC
        );
        assertSurvivors(new ParquetPushedExpressions(List.of(range)), blocks, 5, reusable, new int[] { 2, 3, 4 });
    }

    public void testRangeUpperOnly() {
        // Block: [5, 10, 15, 20, 25]
        long[] values = { 5L, 10L, 15L, 20L, 25L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // x <= 15 -> positions [0, 1, 2]
        Expression range = new Range(
            Source.EMPTY,
            attr("x", DataType.LONG),
            lit(null, DataType.LONG),
            true,
            lit(15L, DataType.LONG),
            true,
            ZoneOffset.UTC
        );
        assertSurvivors(new ParquetPushedExpressions(List.of(range)), blocks, 5, reusable, new int[] { 0, 1, 2 });
    }

    // ---- Test 6: And / Or / Not composition ----

    public void testAndComposition() {
        // Block: [10, 20, 30, 40, 50]
        long[] values = { 10L, 20L, 30L, 40L, 50L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // x > 15 AND x < 45 -> positions [1, 2, 3]
        Expression left = new GreaterThan(Source.EMPTY, attr("x", DataType.LONG), lit(15L, DataType.LONG), null);
        Expression right = new LessThan(Source.EMPTY, attr("x", DataType.LONG), lit(45L, DataType.LONG), null);
        Expression and = new And(Source.EMPTY, left, right);
        assertSurvivors(new ParquetPushedExpressions(List.of(and)), blocks, 5, reusable, new int[] { 1, 2, 3 });
    }

    public void testOrWithBothArmsEvaluable() {
        // Block: [10, 20, 30, 40, 50]
        long[] values = { 10L, 20L, 30L, 40L, 50L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // x == 10 OR x == 50 -> positions [0, 4]
        Expression left = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(10L, DataType.LONG), null);
        Expression right = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(50L, DataType.LONG), null);
        Expression or = new Or(Source.EMPTY, left, right);
        assertSurvivors(new ParquetPushedExpressions(List.of(or)), blocks, 5, reusable, new int[] { 0, 4 });
    }

    public void testOrWithOneArmUnevaluableReturnsNull() {
        // When one arm of OR references a missing column, evaluateExpression returns null for that arm.
        // The OR then returns null (conservative: all rows survive).
        long[] values = { 10L, 20L, 30L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        // Only "x" is in blocks, "y" is missing
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        Expression left = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(10L, DataType.LONG), null);
        Expression right = new Equals(Source.EMPTY, attr("y", DataType.LONG), lit(99L, DataType.LONG), null);
        Expression or = new Or(Source.EMPTY, left, right);

        WordMask result = new ParquetPushedExpressions(List.of(or)).evaluateFilter(blocks, 3, reusable);
        // null return = all rows survive
        assertNull(result);
    }

    public void testNotInvertsMask() {
        // Block: [10, 20, 30, 40, 50]
        long[] values = { 10L, 20L, 30L, 40L, 50L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // NOT(x == 30) -> positions [0, 1, 3, 4]
        Expression inner = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(30L, DataType.LONG), null);
        Expression not = new Not(Source.EMPTY, inner);
        assertSurvivors(new ParquetPushedExpressions(List.of(not)), blocks, 5, reusable, new int[] { 0, 1, 3, 4 });
    }

    // ---- Test 6c: dictionary-match shape classifier and fast-path semantics ----

    public void testDictionaryMatchShapeClassifier() {
        // Pins the gating logic that drives the bulk fast path in applyDictionaryMatches.
        // The behavioural tests below exercise the integrated path; this one isolates the
        // tiny scanner so a regression in either branch (NONE/ALL/MIXED) localizes here.
        assertEquals(
            "empty dictionary maps to NONE (no row can pass)",
            ParquetPushedExpressions.DictionaryMatchShape.NONE,
            ParquetPushedExpressions.classifyDictionaryMatches(new boolean[0])
        );
        assertEquals(
            "all-false maps to NONE",
            ParquetPushedExpressions.DictionaryMatchShape.NONE,
            ParquetPushedExpressions.classifyDictionaryMatches(new boolean[] { false, false, false })
        );
        assertEquals(
            "all-true maps to ALL",
            ParquetPushedExpressions.DictionaryMatchShape.ALL,
            ParquetPushedExpressions.classifyDictionaryMatches(new boolean[] { true, true, true })
        );
        assertEquals(
            "mixed maps to MIXED (early exit after both polarities seen)",
            ParquetPushedExpressions.DictionaryMatchShape.MIXED,
            ParquetPushedExpressions.classifyDictionaryMatches(new boolean[] { true, false, true })
        );
        assertEquals(
            "mixed maps to MIXED regardless of position of the second polarity",
            ParquetPushedExpressions.DictionaryMatchShape.MIXED,
            ParquetPushedExpressions.classifyDictionaryMatches(new boolean[] { false, false, false, true })
        );
    }

    public void testDictionaryFastPathNoMatchProducesEmptyMask() {
        // Equals against a literal absent from the dictionary => dict bitmap is all-false =>
        // applyDictionaryMatches takes the NONE branch and returns the zero-initialized mask
        // without touching the per-row ordinals loop. The mask must be empty (zero survivors),
        // which is the same outcome the per-row loop would produce — so this test is a sound
        // regression guard if the NONE fast path is ever broken.
        String[] dict = { "alpha", "beta", "gamma", "delta" };
        int[] pattern = { 0, 2, 1, 0, 3, 2, 0 };
        int[] ordinals = repeatPattern(pattern, 21);
        Block block = ordinalBlock(dict, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();
            // "never_in_dict" appears nowhere in the dictionary -> dict bitmap is all-false.
            Expression expr = new Equals(
                Source.EMPTY,
                attr("s", DataType.KEYWORD),
                lit(new BytesRef("never_in_dict"), DataType.KEYWORD),
                null
            );
            ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));
            WordMask result = pushed.evaluateFilter(blocks, ordinals.length, reusable);
            assertNotNull(result);
            assertTrue("expected zero survivors for absent literal", result.isEmpty());
        }
    }

    public void testDictionaryFastPathAllMatchReturnsAllSurvivors() {
        // NotEquals against a literal absent from the dictionary => dict bitmap is all-true =>
        // applyDictionaryMatches takes the ALL branch. Every non-null row survives. With the
        // ordinal block built without nulls, evaluateFilter returns null (the all-survive
        // sentinel) — the strongest signal that the bulk shortcut worked end-to-end.
        String[] dict = { "alpha", "beta", "gamma", "delta" };
        int[] pattern = { 0, 2, 1, 0, 3, 2, 0 };
        int[] ordinals = repeatPattern(pattern, 21);
        Block block = ordinalBlock(dict, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();
            Expression expr = new NotEquals(
                Source.EMPTY,
                attr("s", DataType.KEYWORD),
                lit(new BytesRef("never_in_dict"), DataType.KEYWORD),
                null
            );
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, allPositions(ordinals.length));
        }
    }

    public void testDictionaryFastPathAllMatchWithNullsExcludesNullRows() {
        // Dictionary all-true, but the ordinal block has nulls => the ALL branch must still
        // exclude null rows (SQL three-valued logic). Pins the non-null sweep inside the
        // ALL branch against the inverse mistake of bulk-setting every position regardless
        // of the null bitset.
        String[] dict = { "X", "Y" };
        int[] pattern = { 0, 0, 1, 0 };
        boolean[] nullPattern = { false, true, false, false };
        int[] ordinals = repeatPattern(pattern, 25);
        boolean[] nulls = repeatBoolPattern(nullPattern, 25);
        Block block = ordinalBlock(dict, ordinals, nulls);
        try (block) {
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();
            // "Z" is absent from the dictionary -> NotEquals dict bitmap is all-true.
            Expression expr = new NotEquals(Source.EMPTY, attr("s", DataType.KEYWORD), lit(new BytesRef("Z"), DataType.KEYWORD), null);
            int[] expected = positionsWhere(ordinals, nulls, ord -> true);
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, expected);
        }
    }

    public void testDictionaryFastPathEmptyStringNotEquals() {
        // The motivating ClickBench case for the ALL branch: WHERE col != "" on a column whose
        // dictionary contains no empty strings. Without the fast path this still requires an
        // O(rowCount) ordinal-to-boolean mapping despite every row trivially passing the
        // dictionary check; with the fast path it collapses to the null sweep (or setAll for
        // no-nulls). Cross-validates against the per-row path for invariant correctness.
        String[] dict = { "https://elastic.co", "https://kibana.dev", "https://lucene.apache.org" };
        int[] pattern = { 0, 1, 2, 0, 1 };
        int[] ordinals = repeatPattern(pattern, 25);
        Block ordinalsBlock = ordinalBlock(dict, ordinals, null);
        Block plainBlock = plainBytesRefBlock(dict, ordinals);
        try (ordinalsBlock; plainBlock) {
            Expression expr = new NotEquals(Source.EMPTY, attr("url", DataType.KEYWORD), lit(new BytesRef(""), DataType.KEYWORD), null);
            WordMask ordinalMask = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(
                Map.of("url", ordinalsBlock),
                ordinals.length,
                new WordMask()
            );
            WordMask plainMask = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(
                Map.of("url", plainBlock),
                ordinals.length,
                new WordMask()
            );
            // Both paths must produce the same survivor set; nulls in the dictionary fast
            // path get there via maskNonNullRows / the no-null setAll shortcut.
            if (ordinalMask == null) {
                assertNull("dictionary path returned null (all survive); per-row must agree", plainMask);
            } else {
                assertNotNull(plainMask);
                assertArrayEquals(plainMask.survivingPositions(), ordinalMask.survivingPositions());
            }
        }
    }

    public void testIsNotNullFastPathOnBlockWithoutNulls() {
        // Block with no nulls at all => IsNotNull should return a full mask without scanning
        // every position. The mayHaveNulls() gate is what makes this an O(1) decision plus a
        // word-level setAll; without the gate the loop runs rowCount times for the same
        // outcome. Test asserts the survivor set is the full range — semantically what the
        // per-row loop would compute, but the new path is the path under test.
        long[] values = { 1L, 2L, 3L, 4L, 5L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        Expression expr = new IsNotNull(Source.EMPTY, attr("x", DataType.LONG));
        assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, 5, reusable, allPositions(5));
    }

    public void testIsNullFastPathOnBlockWithoutNulls() {
        // Mirror of the IsNotNull case: no nulls => IsNull's survivor set is empty. The fast
        // path returns the zero-initialized mask without iterating; the integration assert is
        // that the result is an empty (non-null) WordMask.
        long[] values = { 1L, 2L, 3L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);

        Expression expr = new IsNull(Source.EMPTY, attr("x", DataType.LONG));
        WordMask result = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(blocks, 3, new WordMask());
        assertNotNull(result);
        assertTrue("IsNull on a no-null block must yield an empty mask", result.isEmpty());
    }

    private static int[] allPositions(int rowCount) {
        int[] out = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            out[i] = i;
        }
        return out;
    }

    // ---- Test 6a: dictionary match memoization reuse across batches within a row group ----

    public void testDictionaryBitmapPopulatedOnFirstUseAndReusedAcrossBatches() {
        // The cache is the iterator's per-row-group memo passed straight into evaluateFilter
        // as a plain map. The first call populates exactly one entry (one pushed predicate);
        // subsequent calls within the same "row group" hit the entry. Observing the map's
        // size after each batch is the cheapest way to assert that the memoization plumbing
        // actually puts (and never re-puts) the bitmap.
        String[] dict = { "apple", "application", "banana", "pineapple" };
        int[] pattern = { 0, 1, 2, 3, 0, 2 };
        int[] ordinals = repeatPattern(pattern, 24);
        Block block = ordinalBlock(dict, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("s", block);
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*app*"));
            ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(like));
            Map<Expression, boolean[]> cache = new IdentityHashMap<>();

            int[] expected = positionsWithOrdinalIn(ordinals, 0, 1, 3);
            for (int batch = 0; batch < 5; batch++) {
                WordMask mask = pushed.evaluateFilter(blocks, ordinals.length, new WordMask(), cache);
                assertNotNull("batch " + batch + " returned null", mask);
                assertArrayEquals("batch " + batch + " survivors changed", expected, mask.survivingPositions());
                assertEquals("cache should hold exactly one entry after batch " + batch, 1, cache.size());
            }
        }
    }

    public void testDictionaryBitmapClearBetweenRowGroupsProducesCorrectResults() {
        // Regression guard for the row-group lifecycle: two consecutive row groups whose
        // dictionaries differ in content but happen to be referenced by the same Expression
        // identity must not share a memoized bitmap. The iterator's responsibility is to
        // pass a different (or cleared) map at the row-group boundary; here we simulate that
        // by clearing the map between calls. Without the clear, the second row group would
        // reuse the first row group's bitmap and return incorrect survivors. With the clear,
        // it recomputes from the second dictionary and produces an empty survivor set.
        String[] firstDict = { "apple", "banana", "cherry", "date" };
        String[] secondDict = { "elderberry", "fig", "grape", "honeydew" };
        int[] ordinals = { 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3 };

        Map<Expression, boolean[]> cache = new IdentityHashMap<>();
        Expression expr = new Equals(Source.EMPTY, attr("s", DataType.KEYWORD), lit(new BytesRef("apple"), DataType.KEYWORD), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(expr));

        Block firstBlock = ordinalBlock(firstDict, ordinals, null);
        try (firstBlock) {
            WordMask firstMask = pushed.evaluateFilter(Map.of("s", firstBlock), ordinals.length, new WordMask(), cache);
            assertNotNull(firstMask);
            // "apple" is ordinal 0 in the first dictionary -> positions 0, 4, 8 survive.
            assertArrayEquals(new int[] { 0, 4, 8 }, firstMask.survivingPositions());
            assertEquals("cache populated for first row group", 1, cache.size());
        }

        // Row-group boundary: the iterator's accessor wipes the map when rowGroupOrdinal
        // changes. Simulate that here. Without this clear the next evaluateFilter would
        // return wrong survivors because the bitmap was computed against the first dictionary.
        cache.clear();

        Block secondBlock = ordinalBlock(secondDict, ordinals, null);
        try (secondBlock) {
            WordMask secondMask = pushed.evaluateFilter(Map.of("s", secondBlock), ordinals.length, new WordMask(), cache);
            assertNotNull(secondMask);
            // "apple" is absent from the second dictionary -> no rows survive.
            assertTrue("expected zero survivors for absent literal in second row group", secondMask.isEmpty());
        }
    }

    public void testDictionaryBitmapCachedAndUncachedAgreeAcrossBatches() {
        // Behavioural cross-check: with and without the cache, every batch must produce the
        // same survivor set. The cache is a pure performance optimization — any divergence
        // means a correctness regression. This is the test most likely to fail if the cache
        // plumbing accidentally serves the wrong bitmap (e.g. identity collision, missed
        // clear, or a wrong key in memoizedDictionaryMatches).
        String[] dict = { "the", "quick", "brown", "fox", "jumps" };
        int rowCount = 200;
        int[] randomOrdinals = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            randomOrdinals[i] = randomIntBetween(0, dict.length - 1);
        }
        Block block = ordinalBlock(dict, randomOrdinals, null);
        try (block) {
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*o*"));
            ParquetPushedExpressions cached = new ParquetPushedExpressions(List.of(like));
            ParquetPushedExpressions uncached = new ParquetPushedExpressions(List.of(like));
            Map<Expression, boolean[]> cache = new IdentityHashMap<>();

            for (int batch = 0; batch < 3; batch++) {
                WordMask cachedMask = cached.evaluateFilter(Map.of("s", block), rowCount, new WordMask(), cache);
                WordMask uncachedMask = uncached.evaluateFilter(Map.of("s", block), rowCount, new WordMask());
                if (cachedMask == null) {
                    assertNull("batch " + batch + ": cached null implies uncached null", uncachedMask);
                } else {
                    assertNotNull("batch " + batch + ": cached non-null, uncached must agree", uncachedMask);
                    assertArrayEquals(
                        "batch " + batch + " survivors diverged between cached and uncached paths",
                        uncachedMask.survivingPositions(),
                        cachedMask.survivingPositions()
                    );
                }
            }
        }
    }

    // ---- Test 6b: empty-mask early exit in evaluateFilter ----

    public void testEvaluateFilterEarlyExitsWhenMaskBecomesEmpty() {
        // Two top-level expressions; the first eliminates every row (no Long equals 999).
        // After the first AND, the survivor mask is empty: any subsequent expression's evaluation
        // is pure waste (notably the per-batch dictionary scan that does not consult the
        // intermediate mask). The early exit short-circuits evaluation of the remainder.
        //
        // Asserted via the lastExpressionsEvaluatedForTesting() hook: only the first expression
        // is evaluated, even though the filter holds two. The mask returned is empty (zero
        // survivors), matching the non-early-exit semantics so behavior is preserved.
        long[] values = { 10L, 20L, 30L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);

        Expression first = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(999L, DataType.LONG), null);
        Expression second = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(20L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(first, second));

        WordMask result = pushed.evaluateFilter(blocks, 3, new WordMask());
        assertNotNull("expected an empty mask, not null", result);
        assertTrue("expected empty mask after first predicate eliminates all rows", result.isEmpty());
        assertEquals("only first expression should have been evaluated", 1, pushed.lastExpressionsEvaluatedForTesting());
    }

    public void testNestedAndShortCircuitsRightWhenLeftIsEmpty() {
        // Inside an And expression, when the left arm eliminates every row in the batch the
        // right arm cannot rescue a single survivor (AND is monotone over the survivor set),
        // so evaluating right is pure waste. The top-level evaluateFilter loop only short-
        // circuits between top-level conjuncts; this test pins the matching short-circuit
        // inside evaluateExpression's And branch, which the planner produces routinely.
        //
        // The Filter holds a single And; semantic output (empty mask) is identical whether
        // or not we short-circuit. Observed via lastEvaluateExpressionCallsForTesting():
        // - And node: 1 call
        // - And.left: 1 call
        // - And.right: 0 calls (short-circuited)
        // Total: 2. Without the short-circuit it would be 3.
        long[] values = { 10L, 20L, 30L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);

        Expression left = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(999L, DataType.LONG), null);
        Expression right = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(20L, DataType.LONG), null);
        Expression nestedAnd = new And(Source.EMPTY, left, right);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(nestedAnd));

        WordMask result = pushed.evaluateFilter(blocks, values.length, new WordMask());
        assertNotNull("expected an empty mask, not null", result);
        assertTrue("expected empty mask: left eliminates all rows", result.isEmpty());
        assertEquals("right arm should not be evaluated when left is already empty", 2, pushed.lastEvaluateExpressionCallsForTesting());
    }

    public void testNestedAndEvaluatesBothArmsWhenLeftIsNonEmpty() {
        // Companion to the short-circuit test: when left admits survivors, right must be
        // evaluated to compute their intersection. Pins that we only skip work when the
        // left arm is fully empty, never when it is just sparse — same contract the outer
        // evaluateFilter loop enforces between top-level conjuncts.
        long[] values = { 10L, 20L, 30L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);

        Expression left = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(20L, DataType.LONG), null);
        Expression right = new Equals(Source.EMPTY, attr("x", DataType.LONG), lit(20L, DataType.LONG), null);
        Expression nestedAnd = new And(Source.EMPTY, left, right);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(nestedAnd));

        WordMask result = pushed.evaluateFilter(blocks, values.length, new WordMask());
        assertNotNull(result);
        assertArrayEquals(new int[] { 1 }, result.survivingPositions());
        assertEquals("both arms should be evaluated when left has survivors", 3, pushed.lastEvaluateExpressionCallsForTesting());
    }

    public void testEvaluateFilterEvaluatesAllExpressionsWhenSurvivorsRemain() {
        // Companion test to the early-exit case: when each predicate leaves at least one
        // survivor, the loop must evaluate every expression. Pins the contract that 6b only
        // skips work when the cumulative mask is fully empty — never when it is just sparse.
        long[] values = { 10L, 20L, 30L, 40L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);

        Expression first = new GreaterThan(Source.EMPTY, attr("x", DataType.LONG), lit(10L, DataType.LONG), null);
        Expression second = new LessThan(Source.EMPTY, attr("x", DataType.LONG), lit(40L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(first, second));

        WordMask result = pushed.evaluateFilter(blocks, 4, new WordMask());
        assertNotNull(result);
        assertArrayEquals(new int[] { 1, 2 }, result.survivingPositions());
        assertEquals("both expressions should have been evaluated", 2, pushed.lastExpressionsEvaluatedForTesting());
    }

    // ---- Test 7: StartsWith ----

    public void testStartsWithBasic() {
        // Block: ["apple", "application", "banana"]
        try (var builder = blockFactory.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("apple"));
            builder.appendBytesRef(new BytesRef("application"));
            builder.appendBytesRef(new BytesRef("banana"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            // STARTS_WITH(s, "app") -> positions [0, 1]
            Expression sw = new StartsWith(Source.EMPTY, attr("s", DataType.KEYWORD), lit(new BytesRef("app"), DataType.KEYWORD));
            assertSurvivors(new ParquetPushedExpressions(List.of(sw)), blocks, 3, reusable, new int[] { 0, 1 });
        }
    }

    public void testStartsWithExactMatch() {
        // Block: ["hello", "world"]
        try (var builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendBytesRef(new BytesRef("hello"));
            builder.appendBytesRef(new BytesRef("world"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            // STARTS_WITH(s, "hello") -> position [0]
            Expression sw = new StartsWith(Source.EMPTY, attr("s", DataType.KEYWORD), lit(new BytesRef("hello"), DataType.KEYWORD));
            assertSurvivors(new ParquetPushedExpressions(List.of(sw)), blocks, 2, reusable, new int[] { 0 });
        }
    }

    public void testStartsWithPrefixLongerThanValue() {
        // Block: ["hi", "hey"]
        try (var builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendBytesRef(new BytesRef("hi"));
            builder.appendBytesRef(new BytesRef("hey"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            // STARTS_WITH(s, "hello_world") -> no matches (prefix longer than values)
            Expression sw = new StartsWith(Source.EMPTY, attr("s", DataType.KEYWORD), lit(new BytesRef("hello_world"), DataType.KEYWORD));
            assertSurvivors(new ParquetPushedExpressions(List.of(sw)), blocks, 2, reusable, new int[] {});
        }
    }

    // ---- Test 8: Missing predicate column (null-constant block) ----

    public void testMissingPredicateColumnWithConstantNullBlock() {
        // When a predicate column is missing from the file, the reader inserts a constant-null block.
        // For a comparison like col > 5, all nulls should yield no matches.
        Block nullBlock = blockFactory.newConstantNullBlock(5);
        Map<String, Block> blocks = Map.of("col", nullBlock);
        WordMask reusable = new WordMask();

        // col > 5 on all-null block -> no survivors
        Expression expr = new GreaterThan(Source.EMPTY, attr("col", DataType.LONG), lit(5L, DataType.LONG), null);
        assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, 5, reusable, new int[] {});
    }

    public void testMissingPredicateColumnIsNullWithConstantNullBlock() {
        // IS NULL on all-null block -> all rows survive
        Block nullBlock = blockFactory.newConstantNullBlock(4);
        Map<String, Block> blocks = Map.of("col", nullBlock);
        WordMask reusable = new WordMask();

        Expression expr = new IsNull(Source.EMPTY, attr("col", DataType.LONG));
        assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, 4, reusable, new int[] { 0, 1, 2, 3 });
    }

    public void testMissingPredicateColumnIsNotNullWithConstantNullBlock() {
        // IS NOT NULL on all-null block -> no rows survive
        Block nullBlock = blockFactory.newConstantNullBlock(4);
        Map<String, Block> blocks = Map.of("col", nullBlock);
        WordMask reusable = new WordMask();

        Expression expr = new IsNotNull(Source.EMPTY, attr("col", DataType.LONG));
        assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, 4, reusable, new int[] {});
    }

    public void testMissingPredicateColumnNotInBlockMap() {
        // When the block is completely absent from the map (null lookup), the evaluator
        // returns null for that expression, meaning all rows survive (conservative).
        Map<String, Block> blocks = new HashMap<>();
        WordMask reusable = new WordMask();

        Expression expr = new GreaterThan(Source.EMPTY, attr("missing", DataType.LONG), lit(5L, DataType.LONG), null);
        WordMask result = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(blocks, 3, reusable);
        // null return means all rows survive
        assertNull(result);
    }

    // ---- Test: evaluateFilter returns null when all rows survive ----

    public void testEvaluateFilterReturnsNullWhenAllRowsSurvive() {
        long[] values = { 10L, 20L, 30L };
        Block block = blockFactory.newLongArrayVector(values, values.length).asBlock();
        Map<String, Block> blocks = Map.of("x", block);
        WordMask reusable = new WordMask();

        // x > 0 -> all rows survive
        Expression expr = new GreaterThan(Source.EMPTY, attr("x", DataType.LONG), lit(0L, DataType.LONG), null);
        WordMask result = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(blocks, 3, reusable);
        assertNull(result);
    }

    // ---- Test 9: OrdinalBytesRefBlock dictionary short-circuit ----

    public void testOrdinalEqualsMatchesDictionaryEntries() {
        // Dictionary: ["alpha", "beta", "gamma", "delta"] (4 entries).
        // Pattern repeated to satisfy OrdinalBytesRefBlock#isDense (rows >= 2 * dictSize),
        // which gates the dictionary short-circuit path.
        int[] pattern = { 0, 2, 1, 0, 3, 2, 0 };
        int[] ordinals = repeatPattern(pattern, 21);
        Block block = ordinalBlock(new String[] { "alpha", "beta", "gamma", "delta" }, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            int[] expected = positionsWithOrdinal(ordinals, 0);
            Expression expr = new Equals(Source.EMPTY, attr("x", DataType.KEYWORD), lit(new BytesRef("alpha"), DataType.KEYWORD), null);
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, expected);
        }
    }

    public void testOrdinalNotEqualsExcludesDictionaryEntries() {
        // x != "beta" -> exclude rows where ordinal == 1.
        int[] pattern = { 0, 1, 2, 3, 0, 1 };
        int[] ordinals = repeatPattern(pattern, 24);
        Block block = ordinalBlock(new String[] { "alpha", "beta", "gamma", "delta" }, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            int[] expected = positionsExcludingOrdinal(ordinals, 1);
            Expression expr = new NotEquals(Source.EMPTY, attr("x", DataType.KEYWORD), lit(new BytesRef("beta"), DataType.KEYWORD), null);
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, expected);
        }
    }

    public void testOrdinalLessThanComparesDictionaryEntries() {
        // Dictionary: ["b", "d", "a", "c"]; "a" < "b" < "c" < "d" lexicographically.
        // x < "c" -> matching entries are "b" (ordinal 0) and "a" (ordinal 2).
        int[] pattern = { 0, 1, 2, 3, 0, 2 };
        int[] ordinals = repeatPattern(pattern, 24);
        Block block = ordinalBlock(new String[] { "b", "d", "a", "c" }, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            int[] expected = positionsWithOrdinalIn(ordinals, 0, 2);
            Expression expr = new LessThan(Source.EMPTY, attr("x", DataType.KEYWORD), lit(new BytesRef("c"), DataType.KEYWORD), null);
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, expected);
        }
    }

    public void testOrdinalInMatchesAnyDictionaryEntry() {
        // x IN ("alpha", "gamma") -> dict matches at ordinals 0, 2.
        int[] pattern = { 0, 1, 2, 3, 0, 2 };
        int[] ordinals = repeatPattern(pattern, 24);
        Block block = ordinalBlock(new String[] { "alpha", "beta", "gamma", "delta" }, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            int[] expected = positionsWithOrdinalIn(ordinals, 0, 2);
            Expression expr = new In(
                Source.EMPTY,
                attr("x", DataType.KEYWORD),
                List.of(lit(new BytesRef("alpha"), DataType.KEYWORD), lit(new BytesRef("gamma"), DataType.KEYWORD))
            );
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, expected);
        }
    }

    // ---- Test: WildcardLike (LIKE) evaluation ----

    public void testWildcardLikeBasicScalar() {
        // ["apple", "application", "banana", "pineapple"], pattern "*app*" matches indexes 0, 1, 3.
        try (var builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendBytesRef(new BytesRef("apple"));
            builder.appendBytesRef(new BytesRef("application"));
            builder.appendBytesRef(new BytesRef("banana"));
            builder.appendBytesRef(new BytesRef("pineapple"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*app*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 4, reusable, new int[] { 0, 1, 3 });
        }
    }

    public void testWildcardLikeMatchAllExcludesNulls() {
        // LIKE "*" must accept every non-null value but reject nulls — SQL three-valued logic
        // says NULL LIKE anything is unknown, not true. The evaluateWildcardLike fast path
        // (matchesAll) is the one under test here; without the explicit null check it would
        // wrongly include null rows.
        try (var builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("bar"));
            builder.appendNull();
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 4, reusable, new int[] { 0, 2 });
        }
    }

    public void testWildcardLikeQuestionMarkSingleChar() {
        // "?" matches exactly one character. "f?o" matches "foo" and "fxo" but not "fo" or "foooo".
        try (var builder = blockFactory.newBytesRefBlockBuilder(5)) {
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendBytesRef(new BytesRef("fxo"));
            builder.appendBytesRef(new BytesRef("fo"));
            builder.appendBytesRef(new BytesRef("foooo"));
            builder.appendBytesRef(new BytesRef("bar"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("f?o"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 5, reusable, new int[] { 0, 1 });
        }
    }

    public void testWildcardLikeNoMatches() {
        // No row matches -> evaluateFilter must return a non-null empty mask (not null, which means
        // "all rows survive"). This guards against confusing the empty-mask and null sentinels.
        try (var builder = blockFactory.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("alpha"));
            builder.appendBytesRef(new BytesRef("beta"));
            builder.appendBytesRef(new BytesRef("gamma"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*xyz*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 3, reusable, new int[] {});
        }
    }

    public void testWildcardLikeCaseInsensitive() {
        // The case-insensitive automaton path goes through RegExp(...) rather than
        // WildcardQuery.toAutomaton; this test pins the contract that case-insensitive matching
        // works end-to-end via the late-mat evaluator.
        try (var builder = blockFactory.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("https://www.GOOGLE.com"));
            builder.appendBytesRef(new BytesRef("https://www.bing.com"));
            builder.appendBytesRef(new BytesRef("https://google.example.com"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("url", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("url", DataType.KEYWORD), new WildcardPattern("*google*"), true);
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 3, reusable, new int[] { 0, 2 });
        }
    }

    public void testWildcardLikeNotInvertsAndExcludesNulls() {
        // Pins SQL three-valued logic for NOT (col LIKE p): a null row evaluates to UNKNOWN
        // and must not survive the predicate, even though its bit in the inner LIKE mask is 0.
        // With ["x.google.com", "x.bing.com", null] and pattern "*google*":
        // LIKE mask = {1, 0, 0} (index 2 is 0 because null, not because no-match)
        // |= null mask = {1, 0, 1}
        // negate = {0, 1, 0} ← survivors
        // Without the Not(WildcardLike) special case in evaluateExpression, the result would be
        // {0, 1, 1} and the YES pushdown would silently change query results around nulls.
        try (var builder = blockFactory.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("x.google.com"));
            builder.appendBytesRef(new BytesRef("x.bing.com"));
            builder.appendNull();
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("url", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("url", DataType.KEYWORD), new WildcardPattern("*google*"));
            Expression not = new Not(Source.EMPTY, like);
            assertSurvivors(new ParquetPushedExpressions(List.of(not)), blocks, 3, reusable, new int[] { 1 });
        }
    }

    public void testWildcardLikeNotOnAllNullColumn() {
        // Pure-null block: NOT (NULL LIKE p) is UNKNOWN for every row, so zero survivors.
        // Exercises the mayHaveNulls() fast path being false (always-null) — guards against
        // the inverse mistake of optimizing away the null scan when nulls are dense.
        try (var builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendNull();
            builder.appendNull();
            builder.appendNull();
            builder.appendNull();
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("url", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("url", DataType.KEYWORD), new WildcardPattern("*google*"));
            Expression not = new Not(Source.EMPTY, like);
            assertSurvivors(new ParquetPushedExpressions(List.of(not)), blocks, 4, reusable, new int[] {});
        }
    }

    public void testWildcardLikeNotWithoutNullsMatchesGenericNegate() {
        // When the block has no nulls, Not(WildcardLike) must agree with the generic "evaluate
        // then negate" path: the null-OR step is a no-op. This locks in that the TVL fix doesn't
        // perturb the no-null common case (e.g. dense URL columns on web-traffic logs).
        try (var builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendBytesRef(new BytesRef("alpha"));
            builder.appendBytesRef(new BytesRef("beta"));
            builder.appendBytesRef(new BytesRef("alphabet"));
            builder.appendBytesRef(new BytesRef("gamma"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("alpha*"));
            Expression not = new Not(Source.EMPTY, like);
            // alpha + alphabet match the inner LIKE; NOT flips to {beta, gamma}.
            assertSurvivors(new ParquetPushedExpressions(List.of(not)), blocks, 4, reusable, new int[] { 1, 3 });
        }
    }

    public void testWildcardLikeWithNulls() {
        // Per SQL three-valued logic, NULL LIKE anything is UNKNOWN, treated as not-matching
        // for filter purposes. Indexes 0 and 4 hold "foobar"/"foo" which match "foo*"; index 2
        // is null and must be excluded.
        try (var builder = blockFactory.newBytesRefBlockBuilder(5)) {
            builder.appendBytesRef(new BytesRef("foobar"));
            builder.appendBytesRef(new BytesRef("bar"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("baz"));
            builder.appendBytesRef(new BytesRef("foo"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("foo*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 5, reusable, new int[] { 0, 4 });
        }
    }

    public void testWildcardLikeWithMissingColumn() {
        // Missing predicate column -> evaluateExpression returns null -> evaluateFilter treats
        // the predicate as "unknown for all rows", which collapses to "all survive" (null return).
        Map<String, Block> blocks = new HashMap<>();
        WordMask reusable = new WordMask();

        Expression like = new WildcardLike(Source.EMPTY, attr("missing", DataType.KEYWORD), new WildcardPattern("*foo*"));
        WordMask result = new ParquetPushedExpressions(List.of(like)).evaluateFilter(blocks, 3, reusable);
        assertNull(result);
    }

    public void testWildcardLikeOrdinalDictionaryShortCircuit() {
        // Dictionary: ["apple", "application", "banana", "pineapple"] (4 entries).
        // Pattern "*app*" matches 0, 1, 3. Ordinal pattern repeated to satisfy
        // OrdinalBytesRefBlock#isDense (rows >= 2 * dictSize) and exercise the dictionary
        // short-circuit, which is the main perf win for high-volume LIKE pushdown.
        String[] dict = { "apple", "application", "banana", "pineapple" };
        int[] pattern = { 0, 1, 2, 3, 0, 2 };
        int[] ordinals = repeatPattern(pattern, 24);
        Block block = ordinalBlock(dict, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();
            int[] expected = positionsWithOrdinalIn(ordinals, 0, 1, 3);
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*app*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, ordinals.length, reusable, expected);
        }
    }

    public void testWildcardLikeOrdinalAndScalarAgree() {
        // Cross-check the dictionary short-circuit against the per-row scalar path on the same
        // logical data. Any divergence between paths indicates a bug in either the dictionary
        // mapping or the scalar loop.
        String[] dict = { "the", "quick", "brown", "fox", "jumps", "over", "the_lazy_dog" };
        int rowCount = 200;
        int[] randomOrdinals = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            randomOrdinals[i] = randomIntBetween(0, dict.length - 1);
        }
        Block ordinalsBlock = ordinalBlock(dict, randomOrdinals, null);
        Block plainBlock = plainBytesRefBlock(dict, randomOrdinals);
        try (ordinalsBlock; plainBlock) {
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*o*"));

            WordMask ordinalMask = new ParquetPushedExpressions(List.of(like)).evaluateFilter(
                Map.of("s", ordinalsBlock),
                rowCount,
                new WordMask()
            );
            WordMask plainMask = new ParquetPushedExpressions(List.of(like)).evaluateFilter(
                Map.of("s", plainBlock),
                rowCount,
                new WordMask()
            );
            if (ordinalMask == null) {
                assertNull("ordinal returned null (all survive); plain must also", plainMask);
            } else {
                assertNotNull("ordinal produced a mask; plain must too", plainMask);
                assertArrayEquals(plainMask.survivingPositions(), ordinalMask.survivingPositions());
            }
        }
    }

    public void testWildcardLikeAutomatonIsCachedAcrossBatches() {
        // The same ParquetPushedExpressions instance is reused across batches. We directly assert
        // memoization via the package-private cache-size hook: after the first evaluateFilter the
        // cache holds exactly one entry, and after subsequent evaluations the size never grows —
        // proving the second-and-later calls hit the cache instead of rebuilding the automaton.
        // Outcome stability is checked too as a regression guard.
        try (var builder = blockFactory.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("apple"));
            builder.appendBytesRef(new BytesRef("application"));
            builder.appendBytesRef(new BytesRef("banana"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);

            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*app*"));
            ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(like));
            assertEquals("cache should start empty", 0, pushed.automatonCacheSizeForTesting());

            for (int i = 0; i < 3; i++) {
                WordMask mask = pushed.evaluateFilter(blocks, 3, new WordMask());
                assertNotNull("iteration " + i + " produced a null mask", mask);
                assertArrayEquals("iteration " + i + " survivors changed", new int[] { 0, 1 }, mask.survivingPositions());
                assertEquals("automaton should be compiled exactly once after iteration " + i, 1, pushed.automatonCacheSizeForTesting());
            }
        }
    }

    public void testWildcardLikeUtf8Bytes() {
        // Pins UTF-32 -> UTF-8 byte handling for non-ASCII characters. The single-arg
        // ByteRunAutomaton constructor used in evaluateWildcardLike performs the conversion
        // internally; using the (Automaton, true) form would silently break this test.
        try (var builder = blockFactory.newBytesRefBlockBuilder(4)) {
            builder.appendBytesRef(new BytesRef("café"));
            builder.appendBytesRef(new BytesRef("Café"));
            builder.appendBytesRef(new BytesRef("naïve"));
            builder.appendBytesRef(new BytesRef("plain"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            WordMask reusable = new WordMask();

            // Case-sensitive "*café*" matches index 0 only — "Café" has uppercase C.
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*café*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 4, new WordMask(), new int[] { 0 });

            // Case-insensitive "*café*" matches both "café" and "Café".
            Expression likeCi = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*café*"), true);
            assertSurvivors(new ParquetPushedExpressions(List.of(likeCi)), blocks, 4, reusable, new int[] { 0, 1 });
        }
    }

    public void testWildcardLikePredicateColumnNamesIncludesField() {
        // Without including the WildcardLike field in predicateColumnNames, the late-mat path
        // would not request the column from the reader and the predicate would always evaluate
        // against a missing block (returning null/all-survive). This test pins the contract.
        Expression like = new WildcardLike(Source.EMPTY, attr("url", DataType.KEYWORD), new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(like));
        assertEquals(java.util.Set.of("url"), pushed.predicateColumnNames());
    }

    public void testExtractContainsLiteralRecognizesPureContainsShape() {
        // Pins the exact set of patterns that take the SIMD-accelerated contains() path. Anything
        // not on this list falls back to the per-byte ByteRunAutomaton; that is correct, but a silent
        // regression here would dial back the dictionary fast-path on URL/path columns and is the
        // single most expensive perf bug this change can introduce.
        assertEquals(new BytesRef("google"), ParquetPushedExpressions.extractContainsLiteral("*google*"));
        // Single-character literal — minimum recognized length is 3 ("*x*").
        assertEquals(new BytesRef("a"), ParquetPushedExpressions.extractContainsLiteral("*a*"));
        // UTF-8 multi-byte: helper must commit to bytes, not codepoints, because the runtime matcher
        // operates on bytes.
        assertEquals(new BytesRef("café"), ParquetPushedExpressions.extractContainsLiteral("*café*"));
    }

    public void testExtractContainsLiteralRejectsNonContainsShapes() {
        // "**" -> middle is empty AND length<3 -> not recognized; the matchesAll automaton fast-path
        // already handles "*", and "**" is equivalent at the automaton level.
        assertNull(ParquetPushedExpressions.extractContainsLiteral("**"));
        // No leading wildcard -> StartsWith territory, not contains.
        assertNull(ParquetPushedExpressions.extractContainsLiteral("google*"));
        // No trailing wildcard -> EndsWith territory, not contains.
        assertNull(ParquetPushedExpressions.extractContainsLiteral("*google"));
        // Embedded '*' -> two-segment pattern; the simple substring check would be wrong.
        assertNull(ParquetPushedExpressions.extractContainsLiteral("*foo*bar*"));
        // Embedded '?' -> single-char wildcard; the simple substring check would be wrong.
        assertNull(ParquetPushedExpressions.extractContainsLiteral("*foo?bar*"));
        // Embedded escape '\\' -> the literal would need un-escaping; keep on the automaton path.
        assertNull(ParquetPushedExpressions.extractContainsLiteral("*fo\\*o*"));
    }

    public void testWildcardLikeContainsLiteralOrdinalDictionaryAgreesWithAutomaton() {
        // Cross-checks the SIMD contains path against the per-row scalar path on a dictionary
        // mixing entries above and below the ~24-byte SIMD activation threshold. Long URLs
        // exercise the Panama path inside ESVectorUtil#contains; the short "google" entry exercises
        // the scalar fallback inside the same helper. Any divergence between the two evaluator
        // paths (ordinal short-circuit vs per-row) surfaces as a failure.
        String[] dict = {
            "https://www.google.com/search?q=elasticsearch",
            "https://github.com/elastic/elasticsearch/issues",
            "https://www.bing.com/search?q=opensearch",
            "https://duckduckgo.com/?q=lucene+vector",
            "https://www.google.com/maps/place/London",
            "https://stackoverflow.com/questions/tagged/lucene",
            "google" };
        int rowCount = 200;
        int[] randomOrdinals = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            randomOrdinals[i] = randomIntBetween(0, dict.length - 1);
        }
        Block ordinalsBlock = ordinalBlock(dict, randomOrdinals, null);
        Block plainBlock = plainBytesRefBlock(dict, randomOrdinals);
        try (ordinalsBlock; plainBlock) {
            // *google* takes the SIMD path; *go?gle* (single-char wildcard) falls back to the
            // automaton. They must agree on every row.
            for (String pattern : new String[] { "*google*", "*google.com*", "*xyz*", "*go?gle*" }) {
                Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern(pattern));

                WordMask ordinalMask = new ParquetPushedExpressions(List.of(like)).evaluateFilter(
                    Map.of("s", ordinalsBlock),
                    rowCount,
                    new WordMask()
                );
                WordMask plainMask = new ParquetPushedExpressions(List.of(like)).evaluateFilter(
                    Map.of("s", plainBlock),
                    rowCount,
                    new WordMask()
                );
                if (ordinalMask == null) {
                    assertNull("pattern [" + pattern + "]: ordinal returned null (all survive); plain must also", plainMask);
                } else {
                    assertNotNull("pattern [" + pattern + "]: ordinal produced a mask; plain must too", plainMask);
                    assertArrayEquals(
                        "pattern [" + pattern + "]: ordinal and plain masks diverge",
                        plainMask.survivingPositions(),
                        ordinalMask.survivingPositions()
                    );
                }
            }
        }
    }

    public void testWildcardLikeContainsLiteralScalarPath() {
        // Direct functional test of the SIMD contains path through a plain BytesRefBlock — values
        // are deliberately mixed: long enough to clear the SIMD activation threshold, short enough
        // to exercise the scalar fallback inside ESVectorUtil#contains, and one null for SQL TVL.
        try (var builder = blockFactory.newBytesRefBlockBuilder(5)) {
            builder.appendBytesRef(new BytesRef("https://www.google.com/maps")); // long, contains "google"
            builder.appendBytesRef(new BytesRef("google")); // short, contains "google"
            builder.appendBytesRef(new BytesRef("https://www.bing.com/")); // long, no "google"
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("Google")); // wrong case, must NOT match case-sensitive *google*
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*google*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 5, new WordMask(), new int[] { 0, 1 });
        }
    }

    public void testWildcardLikeContainsLiteralCaseInsensitiveStillCorrect() {
        // Case-insensitive *literal* is intentionally NOT on the SIMD path (lowercasing the haystack
        // would defeat the SIMD win). It must still produce the case-insensitive answer via the
        // automaton fallback.
        try (var builder = blockFactory.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("https://www.google.com/"));
            builder.appendBytesRef(new BytesRef("https://www.GOOGLE.com/maps"));
            builder.appendBytesRef(new BytesRef("https://www.bing.com/"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*google*"), true);
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 3, new WordMask(), new int[] { 0, 1 });
        }
    }

    public void testWildcardLikeContainsLiteralNotOnOrdinalDictionaryAgreesWithAutomaton() {
        // NOT (col LIKE *literal*) on an OrdinalBytesRefBlock now goes through the SIMD matcher
        // for the inner LIKE before the TVL null-fixup and the negate. The existing NOT tests
        // (testWildcardLikeNotInvertsAndExcludesNulls etc.) only cover plain BytesRefBlocks, so
        // they would not catch a bug where the dictionary scatter or the ordinal-mode null fixup
        // disagreed with the per-row scalar path under the new matcher. Cross-check the two paths
        // on a small fixed layout that includes a null row to exercise SQL three-valued logic.
        String[] dict = { "https://www.google.com/maps", "https://www.bing.com/", "google" };
        // 7 rows: ordinal 0 (match), 1 (no-match), 2 (match), null, 0 (match), 1 (no-match), 2 (match)
        int[] ordinals = { 0, 1, 2, 0, 0, 1, 2 };
        boolean[] nulls = { false, false, false, true, false, false, false };
        Block ordinalsBlock = ordinalBlock(dict, ordinals, nulls);
        // Plain block must reflect the same null mask, not just the ordinals.
        Block plainBlock;
        try (var builder = blockFactory.newBytesRefBlockBuilder(ordinals.length)) {
            for (int i = 0; i < ordinals.length; i++) {
                if (nulls[i]) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(dict[ordinals[i]]));
                }
            }
            plainBlock = builder.build();
        }
        try (ordinalsBlock; plainBlock) {
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*google*"));
            Expression not = new Not(Source.EMPTY, like);

            WordMask ordinalMask = new ParquetPushedExpressions(List.of(not)).evaluateFilter(
                Map.of("s", ordinalsBlock),
                ordinals.length,
                new WordMask()
            );
            WordMask plainMask = new ParquetPushedExpressions(List.of(not)).evaluateFilter(
                Map.of("s", plainBlock),
                ordinals.length,
                new WordMask()
            );
            // Survivors: NOT *google* on dict {0=match, 1=no-match, 2=match} excluding the null row.
            // -> ordinals 1 only (positions 1 and 5).
            int[] expected = new int[] { 1, 5 };
            assertNotNull(ordinalMask);
            assertNotNull(plainMask);
            assertArrayEquals("plain NOT survivors", expected, plainMask.survivingPositions());
            assertArrayEquals("ordinal NOT survivors must agree with plain", expected, ordinalMask.survivingPositions());
        }
    }

    public void testWildcardLikeContainsLiteralEscapeStillCorrect() {
        // *foo\*bar* (literal "foo*bar") must NOT take the SIMD path — extractContainsLiteral
        // returns null when the middle contains '\\'. The automaton fallback still produces the
        // right answer; this test pins that behavior so a future "smarter" un-escape doesn't
        // regress correctness.
        try (var builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendBytesRef(new BytesRef("xfoo*barx"));
            builder.appendBytesRef(new BytesRef("xfooXbarx"));
            Block block = builder.build();
            Map<String, Block> blocks = Map.of("s", block);
            Expression like = new WildcardLike(Source.EMPTY, attr("s", DataType.KEYWORD), new WildcardPattern("*foo\\*bar*"));
            assertSurvivors(new ParquetPushedExpressions(List.of(like)), blocks, 2, new WordMask(), new int[] { 0 });
        }
    }

    public void testOrdinalStartsWithMatchesDictionaryPrefix() {
        // STARTS_WITH(x, "ap") -> matching entries are "apple" (0) and "application" (1).
        int[] pattern = { 0, 1, 2, 0, 2 };
        int[] ordinals = repeatPattern(pattern, 24);
        Block block = ordinalBlock(new String[] { "apple", "application", "banana" }, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            int[] expected = positionsWithOrdinalIn(ordinals, 0, 1);
            Expression expr = new StartsWith(Source.EMPTY, attr("x", DataType.KEYWORD), lit(new BytesRef("ap"), DataType.KEYWORD));
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, expected);
        }
    }

    public void testOrdinalRespectsNullPositions() {
        // Dictionary: ["X", "Y"] with interleaved nulls. Even null rows still occupy ordinal
        // slots in the underlying IntBlock, so the dense check uses the full row count.
        int[] pattern = { 0, 0, 1, 0, 0 };
        boolean[] nullPattern = { false, true, false, false, true };
        int[] ordinals = repeatPattern(pattern, 25);
        boolean[] nulls = repeatBoolPattern(nullPattern, 25);
        Block block = ordinalBlock(new String[] { "X", "Y" }, ordinals, nulls);
        try (block) {
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            int[] expected = positionsWithOrdinalAndNotNull(ordinals, nulls, 0);
            Expression expr = new Equals(Source.EMPTY, attr("x", DataType.KEYWORD), lit(new BytesRef("X"), DataType.KEYWORD), null);
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, expected);
        }
    }

    public void testOrdinalSparseDictionaryFallsBackToScalarPath() {
        // Dictionary size > rowCount / 2 -> OrdinalBytesRefBlock#isDense returns false, so
        // the dictionary short-circuit is skipped. The scalar BytesRefBlock branch must
        // still produce correct results for the same predicate. This test guards the gate.
        // 6-entry dict, 8 rows -> rows < 2 * dictSize, so the block is not dense.
        String[] dict = { "a0", "a1", "a2", "a3", "a4", "a5" };
        int[] ordinals = { 0, 1, 2, 3, 4, 5, 0, 1 };
        Block block = ordinalBlock(dict, ordinals, null);
        try (block) {
            Map<String, Block> blocks = Map.of("x", block);
            WordMask reusable = new WordMask();
            Expression expr = new Equals(Source.EMPTY, attr("x", DataType.KEYWORD), lit(new BytesRef("a3"), DataType.KEYWORD), null);
            assertSurvivors(new ParquetPushedExpressions(List.of(expr)), blocks, ordinals.length, reusable, new int[] { 3 });
        }
    }

    public void testOrdinalShortCircuitMatchesScalarPathSemantics() {
        // Cross-check the dictionary fast path against the scalar per-row path under several
        // shapes: (a) a randomized partial-match case, (b) a deterministic "all rows match"
        // case that exercises evaluateFilter's null-mask optimization, and (c) a deterministic
        // "no rows match" case. Any divergence between the two paths surfaces as a failure.
        String[] dict = { "the", "quick", "brown", "fox", "jumps" };
        int rowCount = 200;

        // (a) Random partial match.
        int[] randomOrdinals = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            randomOrdinals[i] = randomIntBetween(0, dict.length - 1);
        }
        BytesRef randomLiteral = new BytesRef(dict[randomIntBetween(0, dict.length - 1)]);
        assertOrdinalAndScalarAgree(dict, randomOrdinals, randomLiteral);

        // (b) Every row matches the literal -> evaluateFilter should return null (all survive).
        int matchOrdinal = randomIntBetween(0, dict.length - 1);
        int[] allMatchOrdinals = new int[rowCount];
        Arrays.fill(allMatchOrdinals, matchOrdinal);
        assertOrdinalAndScalarAgree(dict, allMatchOrdinals, new BytesRef(dict[matchOrdinal]));

        // (c) No row matches the literal -> evaluateFilter should return an empty mask.
        int presentOrdinal = randomIntBetween(0, dict.length - 1);
        int[] noMatchOrdinals = new int[rowCount];
        Arrays.fill(noMatchOrdinals, presentOrdinal);
        assertOrdinalAndScalarAgree(dict, noMatchOrdinals, new BytesRef("never_in_dict"));
    }

    private void assertOrdinalAndScalarAgree(String[] dict, int[] ordinals, BytesRef literal) {
        int rowCount = ordinals.length;
        Block ordinalBlock = ordinalBlock(dict, ordinals, null);
        Block plainBlock = plainBytesRefBlock(dict, ordinals);
        try (ordinalBlock; plainBlock) {
            Expression expr = new Equals(Source.EMPTY, attr("x", DataType.KEYWORD), lit(literal, DataType.KEYWORD), null);

            WordMask ordinalMask = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(
                Map.of("x", ordinalBlock),
                rowCount,
                new WordMask()
            );
            WordMask plainMask = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(
                Map.of("x", plainBlock),
                rowCount,
                new WordMask()
            );
            if (ordinalMask == null) {
                assertNull("ordinal returned null (all survive); plain must also", plainMask);
            } else {
                assertNotNull("ordinal produced a mask; plain must too", plainMask);
                assertArrayEquals(plainMask.survivingPositions(), ordinalMask.survivingPositions());
            }
        }
    }

    // ---- helpers ----

    /**
     * Builds an {@link OrdinalBytesRefBlock} from a string dictionary plus per-row ordinals
     * and an optional null mask. The block is owned by the caller and must be closed.
     */
    private OrdinalBytesRefBlock ordinalBlock(String[] dict, int[] ordinals, boolean[] nulls) {
        BytesRefVector dictVector = null;
        IntBlock ordinalsBlock = null;
        boolean success = false;
        try (BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(dict.length)) {
            for (String s : dict) {
                dictBuilder.appendBytesRef(new BytesRef(s));
            }
            dictVector = dictBuilder.build();
            try (IntBlock.Builder ordinalsBuilder = blockFactory.newIntBlockBuilder(ordinals.length)) {
                for (int i = 0; i < ordinals.length; i++) {
                    if (nulls != null && nulls[i]) {
                        ordinalsBuilder.appendNull();
                    } else {
                        ordinalsBuilder.appendInt(ordinals[i]);
                    }
                }
                ordinalsBlock = ordinalsBuilder.build();
            }
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalsBlock, dictVector);
            success = true;
            return result;
        } finally {
            if (success == false) {
                if (ordinalsBlock != null) ordinalsBlock.close();
                if (dictVector != null) dictVector.close();
            }
        }
    }

    /**
     * Builds an equivalent plain {@link Block} (BytesRefBlock) by materializing each row.
     * Used by {@link #testOrdinalShortCircuitMatchesScalarPathSemantics} to cross-check
     * that the dictionary fast path yields the same survivors as the per-row path.
     */
    private Block plainBytesRefBlock(String[] dict, int[] ordinals) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(ordinals.length)) {
            for (int o : ordinals) {
                builder.appendBytesRef(new BytesRef(dict[o]));
            }
            return builder.build();
        }
    }

    /** Repeats {@code pattern} {@code times} times to produce a longer ordinal sequence. */
    private static int[] repeatPattern(int[] pattern, int times) {
        int[] out = new int[pattern.length * times];
        for (int t = 0; t < times; t++) {
            System.arraycopy(pattern, 0, out, t * pattern.length, pattern.length);
        }
        return out;
    }

    private static boolean[] repeatBoolPattern(boolean[] pattern, int times) {
        boolean[] out = new boolean[pattern.length * times];
        for (int t = 0; t < times; t++) {
            System.arraycopy(pattern, 0, out, t * pattern.length, pattern.length);
        }
        return out;
    }

    private static int[] positionsWithOrdinal(int[] ordinals, int target) {
        return positionsWhere(ordinals, null, ord -> ord == target);
    }

    private static int[] positionsExcludingOrdinal(int[] ordinals, int excluded) {
        return positionsWhere(ordinals, null, ord -> ord != excluded);
    }

    private static int[] positionsWithOrdinalIn(int[] ordinals, int... targets) {
        return positionsWhere(ordinals, null, ord -> {
            for (int t : targets) {
                if (ord == t) return true;
            }
            return false;
        });
    }

    private static int[] positionsWithOrdinalAndNotNull(int[] ordinals, boolean[] nulls, int target) {
        return positionsWhere(ordinals, nulls, ord -> ord == target);
    }

    private static int[] positionsWhere(int[] ordinals, boolean[] nulls, java.util.function.IntPredicate matcher) {
        int[] tmp = new int[ordinals.length];
        int n = 0;
        for (int i = 0; i < ordinals.length; i++) {
            if ((nulls == null || nulls[i] == false) && matcher.test(ordinals[i])) {
                tmp[n++] = i;
            }
        }
        return Arrays.copyOf(tmp, n);
    }

    private static void assertSurvivors(
        ParquetPushedExpressions pushed,
        Map<String, Block> blocks,
        int rowCount,
        WordMask reusable,
        int[] expectedPositions
    ) {
        WordMask result = pushed.evaluateFilter(blocks, rowCount, reusable);
        if (expectedPositions.length == rowCount) {
            // All rows survive: evaluateFilter may return null (optimization)
            if (result == null) {
                return;
            }
        }
        if (expectedPositions.length == 0) {
            assertNotNull("Expected zero survivors but got null (all survive)", result);
            assertTrue("Expected empty mask but got non-empty", result.isEmpty());
            return;
        }
        assertNotNull("Expected specific survivors but got null (all survive)", result);
        assertArrayEquals(expectedPositions, result.survivingPositions());
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static Literal lit(Object value, DataType type) {
        return new Literal(Source.EMPTY, value, type);
    }
}
