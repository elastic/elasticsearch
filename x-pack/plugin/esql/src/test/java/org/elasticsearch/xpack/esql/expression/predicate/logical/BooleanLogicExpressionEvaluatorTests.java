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
     */
    // rows = left (T,F,N,MV), cols = right (T,F,N,MV)
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

    public void testOracleAgreesWithLiteralTable() {
        BlockFactory factory = blockFactory();
        for (Cell left : Cell.values()) {
            for (Cell right : Cell.values()) {
                try (BooleanBlock lhs = blockForCell(factory, left); BooleanBlock rhs = blockForCell(factory, right)) {
                    assertThat(oracle(BinaryLogicOperation.AND, lhs, rhs, 0), equalTo(AND_TABLE[left.ordinal()][right.ordinal()]));
                    assertThat(oracle(BinaryLogicOperation.OR, lhs, rhs, 0), equalTo(OR_TABLE[left.ordinal()][right.ordinal()]));
                }
            }
        }
    }

    public void testRandomPagesMatchOracle() {
        BlockFactory factory = blockFactory();
        int positions = between(1, 64);
        try (
            BooleanBlock lhs = randomBooleanBlock(factory, positions, true);
            BooleanBlock rhs = randomBooleanBlock(factory, positions, true)
        ) {
            assertEvalMatchesOracle(andLogic(), lhs, rhs);
            assertEvalMatchesOracle(orLogic(), lhs, rhs);
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
     * Values must match; {@code assertSame} is reserved until the short-cut ships.
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
                assertBlocksEqual(result, nullable);
            }
            try (Block result = eval(andLogic(), nullable, allTrue)) {
                assertBlocksEqual(result, nullable);
            }
            try (Block result = eval(orLogic(), allFalse, nullable)) {
                assertBlocksEqual(result, nullable);
            }
            try (Block result = eval(orLogic(), nullable, allFalse)) {
                assertBlocksEqual(result, nullable);
            }
            try (Block result = eval(andLogic(), allFalse, nullable)) {
                assertAllFalse(result, positions);
            }
            try (Block result = eval(andLogic(), nullable, allFalse)) {
                assertAllFalse(result, positions);
            }
            try (Block result = eval(orLogic(), allTrue, nullable)) {
                assertAllTrue(result, positions);
            }
            try (Block result = eval(orLogic(), nullable, allTrue)) {
                assertAllTrue(result, positions);
            }
        }
    }

    /**
     * Mixed MV / scalar / null against an absorbing vector. A short-cut that
     * saw {@code mayHaveMultivaluedFields()} and nulled the whole page would
     * wipe the scalar positions.
     */
    public void testAbsorbingVectorTimesMixedMv() {
        BlockFactory factory = blockFactory();
        int positions = 4;
        try (
            BooleanBlock allFalse = uniformArrayVector(factory, positions, false);
            BooleanBlock allTrue = uniformArrayVector(factory, positions, true);
            BooleanBlock mixed = mixedMvBlock(factory)
        ) {
            assertEvalMatchesOracle(andLogic(), allFalse, mixed);
            assertEvalMatchesOracle(andLogic(), mixed, allFalse);
            assertEvalMatchesOracle(orLogic(), allTrue, mixed);
            assertEvalMatchesOracle(orLogic(), mixed, allTrue);
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
                assertAllFalse(result, positions);
            }
            try (Block result = eval(andLogic(), (BooleanBlock) nulls, allFalse)) {
                assertAllFalse(result, positions);
            }
            try (Block result = eval(andLogic(), allTrue, (BooleanBlock) nulls)) {
                assertAllNull(result, positions);
            }
            try (Block result = eval(orLogic(), allTrue, (BooleanBlock) nulls)) {
                assertAllTrue(result, positions);
            }
            try (Block result = eval(orLogic(), (BooleanBlock) nulls, allTrue)) {
                assertAllTrue(result, positions);
            }
            try (Block result = eval(orLogic(), allFalse, (BooleanBlock) nulls)) {
                assertAllNull(result, positions);
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
     * must still drain the breaker.
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
            try (
                BooleanBlock lhs = positions == 0 ? emptyArrayVector(factory) : randomBooleanBlock(factory, positions, randomBoolean());
                BooleanBlock rhs = positions == 0 ? emptyArrayVector(factory) : randomBooleanBlock(factory, positions, randomBoolean());
                ExpressionEvaluator evaluator = new BooleanLogicExpressionEvaluator(
                    randomBoolean() ? andLogic() : orLogic(),
                    new LoadFromPageEvaluator(0),
                    new LoadFromPageEvaluator(1)
                );
                Block ignored = evaluator.eval(new Page(lhs, rhs))
            ) {}
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
                assertBlocksEqual(result, x);
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

    private static BooleanBlock mixedMvBlock(BlockFactory factory) {
        try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(4)) {
            builder.beginPositionEntry();
            builder.appendBoolean(true);
            builder.appendBoolean(false);
            builder.endPositionEntry();
            builder.appendBoolean(true);
            builder.appendBoolean(false);
            builder.appendNull();
            BooleanBlock block = builder.build();
            assertTrue(block.mayHaveMultivaluedFields());
            return block;
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

    private static void assertBlocksEqual(Block actual, Block expected) {
        assertThat(actual.getPositionCount(), equalTo(expected.getPositionCount()));
        for (int p = 0; p < actual.getPositionCount(); p++) {
            assertThat(BlockUtils.toJavaObject(actual, p), equalTo(BlockUtils.toJavaObject(expected, p)));
        }
    }

    private static void assertEmptyNonConstantArray(Block result) {
        assertThat(result.getPositionCount(), equalTo(0));
        assertThat(result.asVector(), instanceOf(BooleanArrayVector.class));
        assertFalse(result.asVector().isConstant());
    }

    private static void assertAllFalse(Block result, int positions) {
        assertThat(result.getPositionCount(), equalTo(positions));
        BooleanVector vector = (BooleanVector) result.asVector();
        assertNotNull(vector);
        assertTrue(vector.allFalse());
    }

    private static void assertAllTrue(Block result, int positions) {
        assertThat(result.getPositionCount(), equalTo(positions));
        BooleanVector vector = (BooleanVector) result.asVector();
        assertNotNull(vector);
        assertTrue(vector.allTrue());
    }
}
