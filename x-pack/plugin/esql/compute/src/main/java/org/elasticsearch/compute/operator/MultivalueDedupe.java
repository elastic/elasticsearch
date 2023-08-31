/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.function.Supplier;

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
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block, blockFactory).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block, blockFactory).dedupeToBlockAdaptive();
            case INT -> new MultivalueDedupeInt((IntBlock) block, blockFactory).dedupeToBlockAdaptive();
            case LONG -> new MultivalueDedupeLong((LongBlock) block, blockFactory).dedupeToBlockAdaptive();
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block, blockFactory).dedupeToBlockAdaptive();
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
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block, blockFactory).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block, blockFactory).dedupeToBlockUsingCopyMissing();
            case INT -> new MultivalueDedupeInt((IntBlock) block, blockFactory).dedupeToBlockUsingCopyMissing();
            case LONG -> new MultivalueDedupeLong((LongBlock) block, blockFactory).dedupeToBlockUsingCopyMissing();
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block, blockFactory).dedupeToBlockUsingCopyMissing();
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
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block, blockFactory).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block, blockFactory).dedupeToBlockUsingCopyAndSort();
            case INT -> new MultivalueDedupeInt((IntBlock) block, blockFactory).dedupeToBlockUsingCopyAndSort();
            case LONG -> new MultivalueDedupeLong((LongBlock) block, blockFactory).dedupeToBlockUsingCopyAndSort();
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block, blockFactory).dedupeToBlockUsingCopyAndSort();
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Build and {@link EvalOperator.ExpressionEvaluator} that deduplicates values
     * using an adaptive algorithm based on the size of the input list.
     */
    public static Supplier<EvalOperator.ExpressionEvaluator> evaluator(
        ElementType elementType,
        Supplier<EvalOperator.ExpressionEvaluator> nextSupplier
    ) {
        return switch (elementType) {
            case BOOLEAN -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page, BlockFactory blockFactory) {
                    return new MultivalueDedupeBoolean((BooleanBlock) field.eval(page, blockFactory), blockFactory).dedupeToBlock();
                }
            };
            case BYTES_REF -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page, BlockFactory blockFactory) {
                    return new MultivalueDedupeBytesRef((BytesRefBlock) field.eval(page, blockFactory), blockFactory)
                        .dedupeToBlockAdaptive();
                }
            };
            case INT -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page, BlockFactory blockFactory) {
                    return new MultivalueDedupeInt((IntBlock) field.eval(page, blockFactory), blockFactory).dedupeToBlockAdaptive();
                }
            };
            case LONG -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page, BlockFactory blockFactory) {
                    return new MultivalueDedupeLong((LongBlock) field.eval(page, blockFactory), blockFactory).dedupeToBlockAdaptive();
                }
            };
            case DOUBLE -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page, BlockFactory blockFactory) {
                    return new MultivalueDedupeDouble((DoubleBlock) field.eval(page, blockFactory), blockFactory).dedupeToBlockAdaptive();
                }
            };
            case NULL -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page, BlockFactory blockFactory) {
                    return field.eval(page, blockFactory); // The page is all nulls and when you dedupe that it's still all nulls
                }
            };
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
    public static BatchEncoder batchEncoder(Block block, BlockFactory blockFactory, int batchSize) {
        // TODO collect single-valued block handling here. And maybe vector. And maybe all null?
        // TODO check for for unique multivalued fields and for ascending multivalue fields.
        return switch (block.elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block, blockFactory).batchEncoder(batchSize);
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block, blockFactory).batchEncoder(batchSize);
            case INT -> new MultivalueDedupeInt((IntBlock) block, blockFactory).batchEncoder(batchSize);
            case LONG -> new MultivalueDedupeLong((LongBlock) block, blockFactory).batchEncoder(batchSize);
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block, blockFactory).batchEncoder(batchSize);
            default -> throw new IllegalArgumentException();
        };
    }

    private abstract static class MvDedupeEvaluator implements EvalOperator.ExpressionEvaluator {
        protected final EvalOperator.ExpressionEvaluator field;

        private MvDedupeEvaluator(EvalOperator.ExpressionEvaluator field) {
            this.field = field;
        }

        @Override
        public String toString() {
            return "MvDedupe[field=" + field + "]";
        }
    }

    private MultivalueDedupe() {}
}
