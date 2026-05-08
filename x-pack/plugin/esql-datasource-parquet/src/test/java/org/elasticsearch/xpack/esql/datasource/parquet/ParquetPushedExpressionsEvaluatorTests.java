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
