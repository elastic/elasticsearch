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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.junit.Before;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Exhaustive matrix asserting that no pushable predicate can be broken by a batch that decodes
 * entirely null, for any literal type the pushdown admits.
 *
 * <p><b>Why this exists.</b> {@code ConstantNullBlock} implements every typed block interface at
 * once, so it satisfies the first arm of any {@code instanceof}-on-block-class dispatch chain
 * regardless of the column's real type. Where such an arm does typed work on the plan literal
 * above the per-row null guards — as {@code evaluateComparison}, {@code evaluateIn} and
 * {@code evaluateRange} each did — a keyword or boolean predicate over an all-null batch failed
 * the whole query with a {@code ClassCastException} (elastic/elasticsearch#157313). One hand-written
 * regression test covers the reported shape; this matrix covers the shape's whole class, including
 * the arms that are merely shadowed today and would go live on an arm reorder.
 *
 * <p><b>Why the literal axis is enumerated, not listed.</b> The admissible types are read from
 * {@link ParquetFilterPushdownSupport#TYPE_SUPPORTED} rather than hard-coded, and
 * {@link #sampleLiteral} fails loudly on a type it does not know. Adding a type to the pushdown
 * therefore breaks this test until the matrix is extended, which is the intended tripwire.
 *
 * <p><b>Known boundary.</b> The predicate-kind axis IS hand-listed: there is no registry of
 * pushable node kinds ({@code ParquetFilterPushdownSupport#canConvert} is code, not data), so a
 * newly pushable node kind does not enter this matrix automatically. Random composition over the
 * real reader path in {@code ParquetReaderFilterDifferentialTests} is the net for that.
 */
public class ParquetPushedExpressionsNullBatchMatrixTests extends ESTestCase {

    private static final int ROWS = 6;
    private static final String COL = "col";

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * The full cross product: every admissible literal type x every predicate kind x every all-null
     * block shape must evaluate to zero survivors without throwing.
     */
    public void testAllNullBatchYieldsNoSurvivorsAcrossTheMatrix() {
        List<DataType> types = admissibleTypes();
        assertFalse("TYPE_SUPPORTED admitted nothing - the enumeration is broken", types.isEmpty());

        List<String> failures = new ArrayList<>();
        int cells = 0;
        for (DataType type : types) {
            for (BlockShape shape : BlockShape.values()) {
                for (PredicateKind kind : PredicateKind.values()) {
                    cells++;
                    Block block = shape.build(blockFactory, type);
                    try (block) {
                        Expression expr = kind.build(attr(COL, type), sampleLiteral(type), type);
                        Map<String, Block> blocks = Map.of(COL, block);
                        WordMask result = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(blocks, ROWS, new WordMask());
                        if (result == null) {
                            failures.add(cell(type, shape, kind) + ": returned the all-survive sentinel, expected zero survivors");
                        } else if (result.isEmpty() == false) {
                            failures.add(cell(type, shape, kind) + ": survivors " + Arrays.toString(result.survivingPositions()));
                        }
                    } catch (Exception e) {
                        failures.add(cell(type, shape, kind) + ": threw " + e.getClass().getSimpleName() + " " + e.getMessage());
                    }
                }
            }
        }
        assertTrue("matrix did not run", cells > 0);
        assertTrue(cells + " cells, " + failures.size() + " failed:\n" + String.join("\n", failures), failures.isEmpty());
    }

    /**
     * Sanity row for the matrix itself: a valued block of the matching type must still filter
     * normally. Without this, a change that made every evaluator return an empty mask would leave
     * the matrix above green while silently dropping every row in production.
     */
    public void testValuedBlockStillFiltersSoTheMatrixCanFail() {
        Block block = blockFactory.newLongArrayVector(new long[] { 10L, 20L, 30L, 40L, 50L, 60L }, ROWS).asBlock();
        try (block) {
            Expression expr = new Equals(Source.EMPTY, attr(COL, DataType.LONG), lit(30L, DataType.LONG), null);
            WordMask result = new ParquetPushedExpressions(List.of(expr)).evaluateFilter(Map.of(COL, block), ROWS, new WordMask());
            assertNotNull(result);
            assertArrayEquals(new int[] { 2 }, result.survivingPositions());
        }
    }

    private static List<DataType> admissibleTypes() {
        return Arrays.stream(DataType.values()).filter(ParquetFilterPushdownSupport.TYPE_SUPPORTED::test).toList();
    }

    /**
     * A well-formed literal for {@code type}, as the planner would produce. Deliberately total and
     * fail-loud: a new entry in {@link ParquetFilterPushdownSupport#TYPE_SUPPORTED} lands here as a
     * test failure telling the author to extend the matrix.
     */
    private static Object sampleLiteral(DataType type) {
        return switch (type) {
            case INTEGER -> 42;
            case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> 42L;
            case DOUBLE -> 42.0d;
            case KEYWORD -> new BytesRef("US");
            case BOOLEAN -> Boolean.TRUE;
            default -> throw new AssertionError(
                "No sample literal for ["
                    + type
                    + "], which ParquetFilterPushdownSupport.TYPE_SUPPORTED now admits. "
                    + "Extend this matrix rather than narrowing the enumeration."
            );
        };
    }

    /** A second, distinct literal of the same type, for IN lists and range bounds. */
    private static Object otherLiteral(DataType type) {
        return switch (type) {
            case INTEGER -> 99;
            case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> 99L;
            case DOUBLE -> 99.0d;
            case KEYWORD -> new BytesRef("ZZ");
            case BOOLEAN -> Boolean.FALSE;
            default -> throw new AssertionError("No second sample literal for [" + type + "]; extend this matrix.");
        };
    }

    /** The all-null block shapes a predicate column can present at the evaluator. */
    private enum BlockShape {
        /**
         * What the reader emits for an all-null batch and for a column missing from the file. This
         * is the shape that binds every typed interface at once, so it is valid against every
         * literal type by construction - the cross-type misbind case.
         */
        CONSTANT_NULL((bf, type) -> bf.newConstantNullBlock(ROWS)),
        /**
         * A typed array block that happens to be fully null. {@code AbstractArrayBlock} reports
         * {@code areAllValuesNull()} for it, so post-fix it takes the same short-circuit as
         * {@code CONSTANT_NULL} rather than the per-row guards.
         *
         * <p>Its value is as a tripwire on the guard's own shape: narrow the short-circuit to
         * {@code instanceof ConstantNullBlock} and the keyword and boolean RANGE cells fall
         * through {@code evaluateRange}'s Int/Long/Double arms to the all-survive sentinel, and
         * this matrix fails. Those two cells are also why this shape was red on the parent.
         */
        ALL_NULL_ARRAY((bf, type) -> allNullArray(bf, type));

        private final BiFunction<BlockFactory, DataType, Block> factory;

        BlockShape(BiFunction<BlockFactory, DataType, Block> factory) {
            this.factory = factory;
        }

        Block build(BlockFactory bf, DataType type) {
            return factory.apply(bf, type);
        }
    }

    private static Block allNullArray(BlockFactory bf, DataType type) {
        ElementType elementType = switch (type) {
            case INTEGER -> ElementType.INT;
            case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> ElementType.LONG;
            case DOUBLE -> ElementType.DOUBLE;
            case KEYWORD -> ElementType.BYTES_REF;
            case BOOLEAN -> ElementType.BOOLEAN;
            default -> throw new AssertionError("No element type for [" + type + "]; extend this matrix.");
        };
        try (Block.Builder builder = elementType.newBlockBuilder(ROWS, bf)) {
            for (int i = 0; i < ROWS; i++) {
                builder.appendNull();
            }
            return builder.build();
        }
    }

    /**
     * The pushable predicate shapes that reach a value-comparison evaluator. Hand-listed - see the
     * class javadoc's "Known boundary" note.
     */
    private enum PredicateKind {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        IN,
        RANGE,
        NOT_EQUALS_VIA_NOT;

        Expression build(Attribute field, Object literal, DataType type) {
            Literal value = new Literal(Source.EMPTY, literal, type);
            return switch (this) {
                case EQUALS -> new Equals(Source.EMPTY, field, value, null);
                case NOT_EQUALS -> new NotEquals(Source.EMPTY, field, value, null);
                case LESS_THAN -> new LessThan(Source.EMPTY, field, value, null);
                case LESS_THAN_OR_EQUAL -> new LessThanOrEqual(Source.EMPTY, field, value, null);
                case GREATER_THAN -> new GreaterThan(Source.EMPTY, field, value, null);
                case GREATER_THAN_OR_EQUAL -> new GreaterThanOrEqual(Source.EMPTY, field, value, null);
                case IN -> new In(Source.EMPTY, field, List.of(value, new Literal(Source.EMPTY, otherLiteral(type), type)));
                case RANGE -> new Range(
                    Source.EMPTY,
                    field,
                    value,
                    true,
                    new Literal(Source.EMPTY, otherLiteral(type), type),
                    true,
                    ZoneOffset.UTC
                );
                case NOT_EQUALS_VIA_NOT -> new Not(Source.EMPTY, new Equals(Source.EMPTY, field, value, null));
            };
        }
    }

    private static String cell(DataType type, BlockShape shape, PredicateKind kind) {
        return type + "/" + shape + "/" + kind;
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static Literal lit(Object value, DataType type) {
        return new Literal(Source.EMPTY, value, type);
    }
}
