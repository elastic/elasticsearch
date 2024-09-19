/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.mvdedupe;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;

import java.util.function.BiFunction;

/**
 * Utilities to remove duplicates from multivalued fields.
 */
public final class MultivalueDedupe {
    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an adaptive algorithm based on the size of the input list.
     */
    public static Block dedupeToBlockAdaptive(Block block, BlockFactory blockFactory) {
        return switch (block.elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block).dedupeToBlock(blockFactory);
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block).dedupeToBlockAdaptive(blockFactory);
            case INT -> new MultivalueDedupeInt((IntBlock) block).dedupeToBlockAdaptive(blockFactory);
            case LONG -> new MultivalueDedupeLong((LongBlock) block).dedupeToBlockAdaptive(blockFactory);
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block).dedupeToBlockAdaptive(blockFactory);
            case NULL -> block;
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an algorithm with very low overhead but {@code n^2}
     * case complexity for larger. Prefer {@link #dedupeToBlockAdaptive}
     * which picks based on the number of elements at each position.
     */
    public static Block dedupeToBlockUsingCopyMissing(Block block, BlockFactory blockFactory) {
        return switch (block.elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block).dedupeToBlock(blockFactory);
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block).dedupeToBlockUsingCopyMissing(blockFactory);
            case INT -> new MultivalueDedupeInt((IntBlock) block).dedupeToBlockUsingCopyMissing(blockFactory);
            case LONG -> new MultivalueDedupeLong((LongBlock) block).dedupeToBlockUsingCopyMissing(blockFactory);
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block).dedupeToBlockUsingCopyMissing(blockFactory);
            case NULL -> block;
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an algorithm that sorts all values. It has a higher
     * overhead for small numbers of values at each position than
     * {@link #dedupeToBlockUsingCopyMissing} for large numbers of values the
     * performance is dominated by the {@code n*log n} sort. Prefer
     * {@link #dedupeToBlockAdaptive} unless you need the results sorted.
     */
    public static Block dedupeToBlockUsingCopyAndSort(Block block, BlockFactory blockFactory) {
        return switch (block.elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block).dedupeToBlock(blockFactory);
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block).dedupeToBlockUsingCopyAndSort(blockFactory);
            case INT -> new MultivalueDedupeInt((IntBlock) block).dedupeToBlockUsingCopyAndSort(blockFactory);
            case LONG -> new MultivalueDedupeLong((LongBlock) block).dedupeToBlockUsingCopyAndSort(blockFactory);
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block).dedupeToBlockUsingCopyAndSort(blockFactory);
            case NULL -> block;
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Build and {@link ExpressionEvaluator} that deduplicates values
     * using an adaptive algorithm based on the size of the input list.
     */
    public static ExpressionEvaluator.Factory evaluator(ElementType elementType, ExpressionEvaluator.Factory field) {
        return switch (elementType) {
            case BOOLEAN -> new EvaluatorFactory(
                field,
                (blockFactory, block) -> new MultivalueDedupeBoolean((BooleanBlock) block).dedupeToBlock(blockFactory)
            );
            case BYTES_REF -> new EvaluatorFactory(
                field,
                (blockFactory, block) -> new MultivalueDedupeBytesRef((BytesRefBlock) block).dedupeToBlockAdaptive(blockFactory)
            );
            case INT -> new EvaluatorFactory(
                field,
                (blockFactory, block) -> new MultivalueDedupeInt((IntBlock) block).dedupeToBlockAdaptive(blockFactory)
            );
            case LONG -> new EvaluatorFactory(
                field,
                (blockFactory, block) -> new MultivalueDedupeLong((LongBlock) block).dedupeToBlockAdaptive(blockFactory)
            );
            case DOUBLE -> new EvaluatorFactory(
                field,
                (blockFactory, block) -> new MultivalueDedupeDouble((DoubleBlock) block).dedupeToBlockAdaptive(blockFactory)
            );
            case NULL -> field; // The page is all nulls and when you dedupe that it's still all nulls
            default -> throw new IllegalArgumentException("unsupported type [" + elementType + "]");
        };
    }

    /**
     * Result of calling "hash" on a multivalue dedupe.
     */
    public record HashResult(IntBlock ords, boolean sawNull) {}

    /**
     * Build a {@link BatchEncoder} which deduplicates values at each position
     * and then encodes the results into a {@link byte[]} which can be used for
     * things like hashing many fields together.
     */
    public static BatchEncoder batchEncoder(Block block, int batchSize, boolean allowDirectEncoder) {
        if (block.areAllValuesNull()) {
            if (allowDirectEncoder == false) {
                throw new IllegalArgumentException("null blocks can only be directly encoded");
            }
            return new BatchEncoder.DirectNulls(block);
        }
        var elementType = block.elementType();
        if (allowDirectEncoder && block.mvDeduplicated()) {
            return switch (elementType) {
                case BOOLEAN -> new BatchEncoder.DirectBooleans((BooleanBlock) block);
                case BYTES_REF -> new BatchEncoder.DirectBytesRefs((BytesRefBlock) block);
                case INT -> new BatchEncoder.DirectInts((IntBlock) block);
                case LONG -> new BatchEncoder.DirectLongs((LongBlock) block);
                case DOUBLE -> new BatchEncoder.DirectDoubles((DoubleBlock) block);
                default -> throw new IllegalArgumentException("Unknown [" + elementType + "]");
            };
        } else {
            return switch (elementType) {
                case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block).batchEncoder(batchSize);
                case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block).batchEncoder(batchSize);
                case INT -> new MultivalueDedupeInt((IntBlock) block).batchEncoder(batchSize);
                case LONG -> new MultivalueDedupeLong((LongBlock) block).batchEncoder(batchSize);
                case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block).batchEncoder(batchSize);
                default -> throw new IllegalArgumentException();
            };
        }
    }

    private record EvaluatorFactory(ExpressionEvaluator.Factory field, BiFunction<BlockFactory, Block, Block> dedupe)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context.blockFactory(), field.get(context), dedupe);
        }

        @Override
        public String toString() {
            return "MvDedupe[field=" + field + "]";
        }
    }

    private static class Evaluator implements ExpressionEvaluator {
        private final BlockFactory blockFactory;
        private final ExpressionEvaluator field;
        private final BiFunction<BlockFactory, Block, Block> dedupe;

        protected Evaluator(BlockFactory blockFactory, ExpressionEvaluator field, BiFunction<BlockFactory, Block, Block> dedupe) {
            this.blockFactory = blockFactory;
            this.field = field;
            this.dedupe = dedupe;
        }

        @Override
        public Block eval(Page page) {
            try (Block block = field.eval(page)) {
                return dedupe.apply(blockFactory, block);
            }
        }

        @Override
        public String toString() {
            return "MvDedupe[field=" + field + "]";
        }

        @Override
        public void close() {}
    }

    private MultivalueDedupe() {}
}
