/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Direct tests for {@link BooleanLogicExpressionEvaluator}. Kleene 3VL plus MV-first:
 * a multi-valued position is null even when the other side would absorb in 3VL
 * ({@code false AND MV} and {@code true OR MV} are null).
 */
public class BooleanLogicExpressionEvaluatorTests extends ComputeTestCase {
    private enum Cell {
        T,
        F,
        N,
        MV
    }

    /**
     * Hand-written Kleene table with MV-first. Independent of {@link #oracle} so an
     * oracle that skipped the MV check would still disagree with this table.
     * Rows = left (T,F,N,MV), cols = right (T,F,N,MV). n=1 T/F cells are constant
     * vectors, so this pins 3VL plus the short-cut, not the dense {@code &}/{@code |} loop.
     */
    private static final Boolean[][] AND_TABLE = {
        { Boolean.TRUE, Boolean.FALSE, null, null },
        { Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null },
        { null, Boolean.FALSE, null, null },
        { null, null, null, null }, };
    private static final Boolean[][] OR_TABLE = {
        { Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, null },
        { Boolean.TRUE, Boolean.FALSE, null, null },
        { Boolean.TRUE, null, null, null },
        { null, null, null, null }, };

    public void testLiteralTruthTableAnd() {
        assertLiteralTable(andLogic(), AND_TABLE);
    }

    public void testLiteralTruthTableOr() {
        assertLiteralTable(orLogic(), OR_TABLE);
    }

    public void testRandomPagesMatchOracle() {
        BlockFactory factory = blockFactory();
        int positions = between(0, 64);
        for (Shape left : Shape.values()) {
            for (Shape right : Shape.values()) {
                try (BooleanBlock lhs = shape(factory, left, positions); BooleanBlock rhs = shape(factory, right, positions)) {
                    assertEvalMatchesOracle(andLogic(), lhs, rhs);
                    assertEvalMatchesOracle(orLogic(), lhs, rhs);
                }
            }
        }
    }

    public void testDenseVectorsMatchOracle() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 64);
        try (
            BooleanBlock lhs = randomBooleanVectorBlock(factory, positions);
            BooleanBlock rhs = randomBooleanVectorBlock(factory, positions)
        ) {
            assertThat(lhs.asVector(), instanceOf(BooleanArrayVector.class));
            assertThat(rhs.asVector(), instanceOf(BooleanArrayVector.class));
            assertEvalMatchesOracle(andLogic(), lhs, rhs);
            assertEvalMatchesOracle(orLogic(), lhs, rhs);
        }
    }

    /**
     * Vector × MV block, absorbing and identity, both AND and OR, both sides.
     * Omitting the MV-guard on a one-sided uniform short-cut would make
     * {@code false AND MV} false and {@code true OR MV} true.
     */
    public void testVectorTimesMvBlockIsAlwaysNull() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false);
            BooleanBlock allTrue = uniformArrayVector(factory, positions, true);
            BooleanBlock mv = mvBlock(factory, positions)
        ) {
            try (Block result = eval(andLogic(), allFalse, mv)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(andLogic(), allTrue, mv)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(orLogic(), allFalse, mv)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(orLogic(), allTrue, mv)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(andLogic(), mv, allFalse)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(andLogic(), mv, allTrue)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(orLogic(), mv, allFalse)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(orLogic(), mv, allTrue)) {
                assertAllNull(result, positions);
            }
        }
    }

    /**
     * {@code appendNull} does not allocate {@code firstValueIndexes}, so
     * {@code mayHaveMultivaluedFields()} is false and an identity short-cut is legal.
     */
    public void testUniformVectorTimesNullableNoMv() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (
            BooleanBlock allTrue = uniformArrayVector(factory, positions, true);
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false);
            BooleanBlock nullable = nullableNoMv(factory, positions)
        ) {
            assertFalse(nullable.mayHaveMultivaluedFields());
            assertNull(nullable.asVector());
            try (Block result = eval(andLogic(), allTrue, nullable)) {
                assertSame(nullable, result);
            }
            try (Block result = eval(andLogic(), nullable, allTrue)) {
                assertSame(nullable, result);
            }
            try (Block result = eval(orLogic(), allFalse, nullable)) {
                assertSame(nullable, result);
            }
            try (Block result = eval(orLogic(), nullable, allFalse)) {
                assertSame(nullable, result);
            }
            try (Block result = eval(andLogic(), allFalse, nullable)) {
                assertConstantFalse(result, positions);
            }
            try (Block result = eval(andLogic(), nullable, allFalse)) {
                assertConstantFalse(result, positions);
            }
            try (Block result = eval(orLogic(), allTrue, nullable)) {
                assertConstantTrue(result, positions);
            }
            try (Block result = eval(orLogic(), nullable, allTrue)) {
                assertConstantTrue(result, positions);
            }
        }
    }

    /**
     * Mixed MV / scalar / null against a uniform vector. Absorb must not null the
     * whole page. Identity must not return the mixed block — EVAL would emit MV
     * lists where the oracle wants those positions null.
     */
    public void testUniformVectorTimesMixedMv() {
        BlockFactory factory = blockFactory();
        int positions = between(4, 16);
        try (
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false);
            BooleanBlock allTrue = uniformArrayVector(factory, positions, true);
            BooleanBlock mixed = mixedMvBlock(factory, positions)
        ) {
            assertEvalMatchesOracle(andLogic(), allFalse, mixed);
            assertEvalMatchesOracle(andLogic(), mixed, allFalse);
            assertEvalMatchesOracle(orLogic(), allTrue, mixed);
            assertEvalMatchesOracle(orLogic(), mixed, allTrue);
            assertEvalMatchesOracle(andLogic(), allTrue, mixed);
            assertEvalMatchesOracle(andLogic(), mixed, allTrue);
            assertEvalMatchesOracle(orLogic(), allFalse, mixed);
            assertEvalMatchesOracle(orLogic(), mixed, allFalse);
        }
    }

    /**
     * {@code true AND (all-false vector)} must absorb to a constant, not identity-return
     * the n-sized array. Same for {@code false OR (all-true vector)}.
     */
    public void testAbsorbBeatsIdentity() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (
            BooleanBlock allTrue = uniformArrayVector(factory, positions, true);
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false)
        ) {
            try (Block result = eval(andLogic(), allTrue, allFalse)) {
                assertConstantFalse(result, positions);
                assertNotSame(allFalse, result);
            }
            try (Block result = eval(andLogic(), allFalse, allTrue)) {
                assertConstantFalse(result, positions);
                assertNotSame(allFalse, result);
            }
            try (Block result = eval(orLogic(), allFalse, allTrue)) {
                assertConstantTrue(result, positions);
                assertNotSame(allTrue, result);
            }
            try (Block result = eval(orLogic(), allTrue, allFalse)) {
                assertConstantTrue(result, positions);
                assertNotSame(allTrue, result);
            }
        }
    }

    /**
     * Identity on dense vectors returns the eval {@link Block}, never
     * {@code vector.asBlock()} (a second wrapper would double-close).
     */
    public void testDenseVectorIdentity() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (
            BooleanBlock allTrue = uniformArrayVector(factory, positions, true);
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false);
            BooleanBlock mixed = mixedArrayVector(factory, positions)
        ) {
            try (Block result = eval(andLogic(), allTrue, mixed)) {
                assertSame(mixed, result);
            }
            try (Block result = eval(andLogic(), mixed, allTrue)) {
                assertSame(mixed, result);
            }
            try (Block result = eval(orLogic(), allFalse, mixed)) {
                assertSame(mixed, result);
            }
            try (Block result = eval(orLogic(), mixed, allFalse)) {
                assertSame(mixed, result);
            }
        }
    }

    public void testConstantVectorTimesDense() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (
            BooleanBlock constTrue = factory.newConstantBooleanBlockWith(true, positions);
            BooleanBlock constFalse = factory.newConstantBooleanBlockWith(false, positions);
            BooleanBlock mixed = mixedArrayVector(factory, positions)
        ) {
            try (Block result = eval(andLogic(), constTrue, mixed)) {
                assertSame(mixed, result);
            }
            try (Block result = eval(andLogic(), mixed, constTrue)) {
                assertSame(mixed, result);
            }
            try (Block result = eval(orLogic(), constFalse, mixed)) {
                assertSame(mixed, result);
            }
            try (Block result = eval(orLogic(), mixed, constFalse)) {
                assertSame(mixed, result);
            }
            try (Block result = eval(andLogic(), constFalse, mixed)) {
                assertConstantFalse(result, positions);
            }
            try (Block result = eval(andLogic(), mixed, constFalse)) {
                assertConstantFalse(result, positions);
            }
            try (Block result = eval(orLogic(), constTrue, mixed)) {
                assertConstantTrue(result, positions);
            }
            try (Block result = eval(orLogic(), mixed, constTrue)) {
                assertConstantTrue(result, positions);
            }
        }
    }

    /**
     * {@code ConstantNullBlock.asVector()} is null, so this is the empty non-vector
     * path. Dropping the outer {@code n==0} guard would absorb an empty vector to a
     * 0-sized constant.
     */
    public void testEmptyConstantNullTimesEmptyVector() {
        BlockFactory factory = blockFactory();
        try (
            BooleanBlock emptyVector = emptyArrayVector(factory);
            BooleanBlock emptyNulls = (BooleanBlock) factory.newConstantNullBlock(0)
        ) {
            try (Block and = eval(andLogic(), emptyNulls, emptyVector); Block or = eval(orLogic(), emptyNulls, emptyVector)) {
                assertEmptyNonConstantArray(and);
                assertEmptyNonConstantArray(or);
            }
        }
    }

    /**
     * Both children run even when lhs absorbs. Warnings come from each child's own
     * {@code Warnings}; skipping {@code rightEval} would drop them. Eval count is
     * the proxy for that.
     */
    public void testBothChildrenAlwaysEval() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false);
            BooleanBlock mixed = mixedArrayVector(factory, positions)
        ) {
            AtomicInteger left = new AtomicInteger();
            AtomicInteger right = new AtomicInteger();
            try (
                ExpressionEvaluator evaluator = new BooleanLogicExpressionEvaluator(
                    andLogic(),
                    new CountingEvaluator(new LoadFromPageEvaluator(0), left),
                    new CountingEvaluator(new LoadFromPageEvaluator(1), right)
                );
                Block ignored = evaluator.eval(new Page(allFalse, mixed))
            ) {
                assertThat(left.get(), equalTo(1));
                assertThat(right.get(), equalTo(1));
            }
        }
    }

    public void testConstantNullTimesAbsorbingVector() {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false);
            BooleanBlock allTrue = uniformArrayVector(factory, positions, true);
            Block nulls = factory.newConstantNullBlock(positions)
        ) {
            try (Block result = eval(andLogic(), allFalse, (BooleanBlock) nulls)) {
                assertConstantFalse(result, positions);
            }
            try (Block result = eval(andLogic(), (BooleanBlock) nulls, allFalse)) {
                assertConstantFalse(result, positions);
            }
            try (Block result = eval(andLogic(), allTrue, (BooleanBlock) nulls)) {
                assertSame(nulls, result);
            }
            try (Block result = eval(andLogic(), (BooleanBlock) nulls, allTrue)) {
                assertSame(nulls, result);
            }
            try (Block result = eval(orLogic(), allTrue, (BooleanBlock) nulls)) {
                assertConstantTrue(result, positions);
            }
            try (Block result = eval(orLogic(), (BooleanBlock) nulls, allTrue)) {
                assertConstantTrue(result, positions);
            }
            try (Block result = eval(orLogic(), allFalse, (BooleanBlock) nulls)) {
                assertSame(nulls, result);
            }
            try (Block result = eval(orLogic(), (BooleanBlock) nulls, allFalse)) {
                assertSame(nulls, result);
            }
        }
    }

    public void testEmptyArrayVectorsStayNonConstant() {
        BlockFactory factory = blockFactory();
        try (BooleanBlock lhs = emptyArrayVector(factory); BooleanBlock rhs = emptyArrayVector(factory)) {
            assertThat(lhs.asVector(), instanceOf(BooleanArrayVector.class));
            assertFalse(lhs.asVector().isConstant());
            try (Block and = eval(andLogic(), lhs, rhs); Block or = eval(orLogic(), lhs, rhs)) {
                assertEmptyNonConstantArray(and);
                assertEmptyNonConstantArray(or);
            }
        }
    }

    /**
     * Same channel twice: {@link LoadFromPageEvaluator} {@code incRef}s twice. An identity
     * short-cut would {@code incRef} a third time. Closing the result and the page block
     * must still drain the breaker. Mixed random input almost never hits identity;
     * that rail is {@link #testSameChannelAllTrueAnd} / {@link #testSameChannelAllFalseOr}.
     */
    public void testSameChannelAnd() {
        BlockFactory factory = blockFactory();
        try (BooleanBlock x = randomBooleanBlock(factory, between(1, 32), false)) {
            try (
                ExpressionEvaluator evaluator = new BooleanLogicExpressionEvaluator(
                    andLogic(),
                    new LoadFromPageEvaluator(0),
                    new LoadFromPageEvaluator(0)
                );
                Block result = evaluator.eval(new Page(x))
            ) {
                for (int p = 0; p < x.getPositionCount(); p++) {
                    assertThat(BlockUtils.toJavaObject(result, p), equalTo(oracle(BinaryLogicOperation.AND, x, x, p)));
                }
            }
        }
    }

    public void testSameChannelAllTrueAnd() {
        assertSameChannel(andLogic(), true);
    }

    public void testSameChannelAllFalseOr() {
        assertSameChannel(orLogic(), false);
    }

    public void testCranky() {
        testWithCrankyBlockFactory(factory -> {
            int positions = between(0, 16);
            for (Shape left : Shape.values()) {
                for (Shape right : Shape.values()) {
                    try (BooleanBlock lhs = shape(factory, left, positions); BooleanBlock rhs = shape(factory, right, positions)) {
                        for (BinaryLogic bl : new BinaryLogic[] { andLogic(), orLogic() }) {
                            try (
                                ExpressionEvaluator evaluator = new BooleanLogicExpressionEvaluator(
                                    bl,
                                    new LoadFromPageEvaluator(0),
                                    new LoadFromPageEvaluator(1)
                                );
                                Block ignored = evaluator.eval(new Page(lhs, rhs))
                            ) {}
                        }
                    }
                }
            }
        });
    }

    private void assertSameChannel(BinaryLogic bl, boolean value) {
        BlockFactory factory = blockFactory();
        int positions = between(2, 16);
        try (BooleanBlock x = uniformArrayVector(factory, positions, value)) {
            try (
                ExpressionEvaluator evaluator = new BooleanLogicExpressionEvaluator(
                    bl,
                    new LoadFromPageEvaluator(0),
                    new LoadFromPageEvaluator(0)
                );
                Block result = evaluator.eval(new Page(x))
            ) {
                assertSame(x, result);
            }
        }
    }

    private void assertLiteralTable(BinaryLogic bl, Boolean[][] table) {
        BlockFactory factory = blockFactory();
        Cell[] cells = Cell.values();
        for (int i = 0; i < cells.length; i++) {
            for (int j = 0; j < cells.length; j++) {
                try (BooleanBlock lhs = blockForCell(factory, cells[i]); BooleanBlock rhs = blockForCell(factory, cells[j])) {
                    try (Block result = eval(bl, lhs, rhs)) {
                        assertThat(
                            cells[i] + " " + bl.function() + " " + cells[j],
                            BlockUtils.toJavaObject(result, 0),
                            equalTo(table[i][j])
                        );
                        assertThat(oracle(bl.function(), lhs, rhs, 0), equalTo(table[i][j]));
                    }
                }
            }
        }
    }

    private void assertEvalMatchesOracle(BinaryLogic bl, BooleanBlock lhs, BooleanBlock rhs) {
        try (Block result = eval(bl, lhs, rhs)) {
            assertThat(result.getPositionCount(), equalTo(lhs.getPositionCount()));
            for (int p = 0; p < lhs.getPositionCount(); p++) {
                assertThat(BlockUtils.toJavaObject(result, p), equalTo(oracle(bl.function(), lhs, rhs, p)));
            }
        }
    }

    private static Boolean oracle(BinaryLogicOperation op, Block lhs, Block rhs, int p) {
        if (lhs.getValueCount(p) > 1) {
            return null;
        }
        if (rhs.getValueCount(p) > 1) {
            return null;
        }
        Boolean l = lhs.isNull(p) ? null : ((BooleanBlock) lhs).getBoolean(lhs.getFirstValueIndex(p));
        Boolean r = rhs.isNull(p) ? null : ((BooleanBlock) rhs).getBoolean(rhs.getFirstValueIndex(p));
        return op.apply(l, r);
    }

    private Block eval(BinaryLogic bl, BooleanBlock lhs, BooleanBlock rhs) {
        try (
            ExpressionEvaluator evaluator = new BooleanLogicExpressionEvaluator(
                bl,
                new LoadFromPageEvaluator(0),
                new LoadFromPageEvaluator(1)
            )
        ) {
            return evaluator.eval(new Page(lhs, rhs));
        }
    }

    private static BinaryLogic andLogic() {
        return new And(Source.EMPTY, Literal.TRUE, Literal.TRUE);
    }

    private static BinaryLogic orLogic() {
        return new Or(Source.EMPTY, Literal.TRUE, Literal.TRUE);
    }

    private static BooleanBlock blockForCell(BlockFactory factory, Cell cell) {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(1)) {
            appendCell(builder, cell);
            return builder.build();
        }
    }

    private static void appendCell(BooleanBlock.Builder builder, Cell cell) {
        switch (cell) {
            case T -> builder.appendBoolean(true);
            case F -> builder.appendBoolean(false);
            case N -> builder.appendNull();
            case MV -> {
                builder.beginPositionEntry();
                builder.appendBoolean(true);
                builder.appendBoolean(false);
                builder.endPositionEntry();
            }
        }
    }

    private static BooleanBlock randomBooleanBlock(BlockFactory factory, int positions, boolean allowMv) {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                // 0,1 boolean (50%); 2 null (25%); 3 MV when allowed (25%, else boolean)
                int roll = allowMv ? randomInt(3) : randomInt(2);
                switch (roll) {
                    case 0, 1 -> builder.appendBoolean(randomBoolean());
                    case 2 -> builder.appendNull();
                    case 3 -> {
                        builder.beginPositionEntry();
                        builder.appendBoolean(randomBoolean());
                        builder.appendBoolean(randomBoolean());
                        builder.endPositionEntry();
                    }
                    default -> throw new AssertionError("unexpected roll " + roll);
                }
            }
            return builder.build();
        }
    }

    private static BooleanBlock randomBooleanVectorBlock(BlockFactory factory, int positions) {
        try (BooleanVector.FixedBuilder builder = factory.newBooleanVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendBoolean(i, randomBoolean());
            }
            return builder.build().asBlock();
        }
    }

    private static BooleanBlock uniformArrayVector(BlockFactory factory, int positions, boolean value) {
        try (BooleanVector.FixedBuilder builder = factory.newBooleanVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendBoolean(i, value);
            }
            BooleanBlock block = builder.build().asBlock();
            assertThat(block.asVector().allTrue(), equalTo(value));
            assertThat(block.asVector().allFalse(), equalTo(value == false));
            return block;
        }
    }

    private static BooleanBlock emptyArrayVector(BlockFactory factory) {
        try (BooleanVector.FixedBuilder builder = factory.newBooleanVectorFixedBuilder(0)) {
            return builder.build().asBlock();
        }
    }

    private static BooleanBlock mvBlock(BlockFactory factory, int positions) {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.beginPositionEntry();
                builder.appendBoolean(true);
                builder.appendBoolean(false);
                builder.endPositionEntry();
            }
            BooleanBlock block = builder.build();
            assertTrue(block.mayHaveMultivaluedFields());
            assertNull(block.asVector());
            return block;
        }
    }

    private static BooleanBlock mixedMvBlock(BlockFactory factory, int positions) {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                switch (i % 4) {
                    case 0 -> {
                        builder.beginPositionEntry();
                        builder.appendBoolean(true);
                        builder.appendBoolean(false);
                        builder.endPositionEntry();
                    }
                    case 1 -> builder.appendBoolean(true);
                    case 2 -> builder.appendBoolean(false);
                    case 3 -> builder.appendNull();
                    default -> throw new AssertionError("unexpected remainder");
                }
            }
            BooleanBlock block = builder.build();
            if (positions > 0) {
                assertTrue(block.mayHaveMultivaluedFields());
            }
            return block;
        }
    }

    private static BooleanBlock mixedArrayVector(BlockFactory factory, int positions) {
        try (BooleanVector.FixedBuilder builder = factory.newBooleanVectorFixedBuilder(positions)) {
            builder.appendBoolean(0, true);
            builder.appendBoolean(1, false);
            for (int i = 2; i < positions; i++) {
                builder.appendBoolean(i, randomBoolean());
            }
            BooleanBlock block = builder.build().asBlock();
            assertFalse(block.asVector().allTrue());
            assertFalse(block.asVector().allFalse());
            return block;
        }
    }

    private enum Shape {
        DENSE_VECTOR,
        UNIFORM_TRUE,
        UNIFORM_FALSE,
        CONSTANT_TRUE,
        CONSTANT_FALSE,
        CONSTANT_NULL,
        NULLABLE_NO_MV,
        MIXED_MV,
        RANDOM_BLOCK
    }

    private static BooleanBlock shape(BlockFactory factory, Shape shape, int positions) {
        return switch (shape) {
            case DENSE_VECTOR -> positions == 0 ? emptyArrayVector(factory) : randomBooleanVectorBlock(factory, positions);
            case UNIFORM_TRUE -> positions == 0 ? emptyArrayVector(factory) : uniformArrayVector(factory, positions, true);
            case UNIFORM_FALSE -> positions == 0 ? emptyArrayVector(factory) : uniformArrayVector(factory, positions, false);
            case CONSTANT_TRUE -> factory.newConstantBooleanBlockWith(true, positions);
            case CONSTANT_FALSE -> factory.newConstantBooleanBlockWith(false, positions);
            case CONSTANT_NULL -> (BooleanBlock) factory.newConstantNullBlock(positions);
            case NULLABLE_NO_MV -> nullableNoMv(factory, positions);
            case MIXED_MV -> mixedMvBlock(factory, positions);
            case RANDOM_BLOCK -> positions == 0 ? emptyArrayVector(factory) : randomBooleanBlock(factory, positions, true);
        };
    }

    private record CountingEvaluator(ExpressionEvaluator inner, AtomicInteger count) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            count.incrementAndGet();
            return inner.eval(page);
        }

        @Override
        public long baseRamBytesUsed() {
            return inner.baseRamBytesUsed();
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    private static BooleanBlock nullableNoMv(BlockFactory factory, int positions) {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                if (i % 2 == 0) {
                    builder.appendNull();
                } else {
                    builder.appendBoolean(i % 4 == 1);
                }
            }
            return builder.build();
        }
    }

    private static void assertAllNull(Block result, int positions) {
        assertThat(result.getPositionCount(), equalTo(positions));
        for (int p = 0; p < positions; p++) {
            assertThat(BlockUtils.toJavaObject(result, p), nullValue());
        }
    }

    private static void assertEmptyNonConstantArray(Block result) {
        assertThat(result.getPositionCount(), equalTo(0));
        assertThat(result.asVector(), instanceOf(BooleanArrayVector.class));
        assertFalse(result.asVector().isConstant());
    }

    private static void assertConstantFalse(Block result, int positions) {
        assertThat(result.getPositionCount(), equalTo(positions));
        BooleanVector vector = (BooleanVector) result.asVector();
        assertNotNull(vector);
        assertTrue(vector.allFalse());
        assertTrue(vector.isConstant());
    }

    private static void assertConstantTrue(Block result, int positions) {
        assertThat(result.getPositionCount(), equalTo(positions));
        BooleanVector vector = (BooleanVector) result.asVector();
        assertNotNull(vector);
        assertTrue(vector.allTrue());
        assertTrue(vector.isConstant());
    }
}
