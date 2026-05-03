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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
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

    // ---- helpers ----

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
