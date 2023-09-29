/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;

/**
 * Utilities to remove duplicates from multivalued fields.
 */
public final class MultivalueDedupe {
    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an adaptive algorithm based on the size of the input list.
     */
    public static Block.Ref dedupeToBlockAdaptive(Block.Ref ref) {
        return switch (ref.block().elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean(ref).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef(ref).dedupeToBlockAdaptive();
            case INT -> new MultivalueDedupeInt(ref).dedupeToBlockAdaptive();
            case LONG -> new MultivalueDedupeLong(ref).dedupeToBlockAdaptive();
            case DOUBLE -> new MultivalueDedupeDouble(ref).dedupeToBlockAdaptive();
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an algorithm with very low overhead but {@code n^2}
     * case complexity for larger. Prefer {@link #dedupeToBlockAdaptive}
     * which picks based on the number of elements at each position.
     */
    public static Block.Ref dedupeToBlockUsingCopyMissing(Block.Ref ref) {
        return switch (ref.block().elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean(ref).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef(ref).dedupeToBlockUsingCopyMissing();
            case INT -> new MultivalueDedupeInt(ref).dedupeToBlockUsingCopyMissing();
            case LONG -> new MultivalueDedupeLong(ref).dedupeToBlockUsingCopyMissing();
            case DOUBLE -> new MultivalueDedupeDouble(ref).dedupeToBlockUsingCopyMissing();
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
    public static Block.Ref dedupeToBlockUsingCopyAndSort(Block.Ref ref) {
        return switch (ref.block().elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean(ref).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef(ref).dedupeToBlockUsingCopyAndSort();
            case INT -> new MultivalueDedupeInt(ref).dedupeToBlockUsingCopyAndSort();
            case LONG -> new MultivalueDedupeLong(ref).dedupeToBlockUsingCopyAndSort();
            case DOUBLE -> new MultivalueDedupeDouble(ref).dedupeToBlockUsingCopyAndSort();
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Build and {@link EvalOperator.ExpressionEvaluator} that deduplicates values
     * using an adaptive algorithm based on the size of the input list.
     */
    public static ExpressionEvaluator.Factory evaluator(ElementType elementType, ExpressionEvaluator.Factory nextSupplier) {
        return switch (elementType) {
            case BOOLEAN -> dvrCtx -> new MvDedupeEvaluator(nextSupplier.get(dvrCtx)) {
                @Override
                public Block.Ref eval(Page page) {
                    return new MultivalueDedupeBoolean(field.eval(page)).dedupeToBlock();
                }
            };
            case BYTES_REF -> dvrCtx -> new MvDedupeEvaluator(nextSupplier.get(dvrCtx)) {
                @Override
                public Block.Ref eval(Page page) {
                    return new MultivalueDedupeBytesRef(field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case INT -> dvrCtx -> new MvDedupeEvaluator(nextSupplier.get(dvrCtx)) {
                @Override
                public Block.Ref eval(Page page) {
                    return new MultivalueDedupeInt(field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case LONG -> dvrCtx -> new MvDedupeEvaluator(nextSupplier.get(dvrCtx)) {
                @Override
                public Block.Ref eval(Page page) {
                    return new MultivalueDedupeLong(field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case DOUBLE -> dvrCtx -> new MvDedupeEvaluator(nextSupplier.get(dvrCtx)) {
                @Override
                public Block.Ref eval(Page page) {
                    return new MultivalueDedupeDouble(field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case NULL -> dvrCtx -> new MvDedupeEvaluator(nextSupplier.get(dvrCtx)) {
                @Override
                public Block.Ref eval(Page page) {
                    return field.eval(page); // The page is all nulls and when you dedupe that it's still all nulls
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
    public static BatchEncoder batchEncoder(Block.Ref ref, int batchSize, boolean allowDirectEncoder) {
        var elementType = ref.block().elementType();
        if (allowDirectEncoder && ref.block().mvDeduplicated()) {
            var block = ref.block();
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
                case BOOLEAN -> new MultivalueDedupeBoolean(ref).batchEncoder(batchSize);
                case BYTES_REF -> new MultivalueDedupeBytesRef(ref).batchEncoder(batchSize);
                case INT -> new MultivalueDedupeInt(ref).batchEncoder(batchSize);
                case LONG -> new MultivalueDedupeLong(ref).batchEncoder(batchSize);
                case DOUBLE -> new MultivalueDedupeDouble(ref).batchEncoder(batchSize);
                default -> throw new IllegalArgumentException();
            };
        }
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

        @Override
        public void close() {}
    }

    private MultivalueDedupe() {}
}
