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

import java.util.function.Supplier;

public final class MultivalueDedupe {
    /**
     * Dedupe values using an adaptive algorithm based on the size of the input list.
     */
    public static Block dedupeToBlockAdaptive(Block block) {
        return switch (block.elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block).dedupeToBlockAdaptive();
            case INT -> new MultivalueDedupeInt((IntBlock) block).dedupeToBlockAdaptive();
            case LONG -> new MultivalueDedupeLong((LongBlock) block).dedupeToBlockAdaptive();
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block).dedupeToBlockAdaptive();
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Dedupe values using an {@code n^2} strategy with low overhead. Prefer {@link #dedupeToBlockAdaptive}.
     * This is public for testing and performance testing.
     */
    public static Block dedupeToBlockUsingCopyMissing(Block block) {
        return switch (block.elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block).dedupeToBlockUsingCopyMissing();
            case INT -> new MultivalueDedupeInt((IntBlock) block).dedupeToBlockUsingCopyMissing();
            case LONG -> new MultivalueDedupeLong((LongBlock) block).dedupeToBlockUsingCopyMissing();
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block).dedupeToBlockUsingCopyMissing();
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Dedupe values using an {@code n^2} strategy with low overhead. Prefer {@link #dedupeToBlockAdaptive}.
     * This is public for testing and performance testing.
     */
    public static Block dedupeToBlockUsingCopyAndSort(Block block) {
        return switch (block.elementType()) {
            case BOOLEAN -> new MultivalueDedupeBoolean((BooleanBlock) block).dedupeToBlock();
            case BYTES_REF -> new MultivalueDedupeBytesRef((BytesRefBlock) block).dedupeToBlockUsingCopyAndSort();
            case INT -> new MultivalueDedupeInt((IntBlock) block).dedupeToBlockUsingCopyAndSort();
            case LONG -> new MultivalueDedupeLong((LongBlock) block).dedupeToBlockUsingCopyAndSort();
            case DOUBLE -> new MultivalueDedupeDouble((DoubleBlock) block).dedupeToBlockUsingCopyAndSort();
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Build and {@link EvalOperator.ExpressionEvaluator} that deduplicates values.
     */
    public static Supplier<EvalOperator.ExpressionEvaluator> evaluator(
        ElementType elementType,
        Supplier<EvalOperator.ExpressionEvaluator> nextSupplier
    ) {
        return switch (elementType) {
            case BOOLEAN -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page) {
                    return new MultivalueDedupeBoolean((BooleanBlock) field.eval(page)).dedupeToBlock();
                }
            };
            case BYTES_REF -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page) {
                    return new MultivalueDedupeBytesRef((BytesRefBlock) field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case INT -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page) {
                    return new MultivalueDedupeInt((IntBlock) field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case LONG -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page) {
                    return new MultivalueDedupeLong((LongBlock) field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case DOUBLE -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page) {
                    return new MultivalueDedupeDouble((DoubleBlock) field.eval(page)).dedupeToBlockAdaptive();
                }
            };
            case NULL -> () -> new MvDedupeEvaluator(nextSupplier.get()) {
                @Override
                public Block eval(Page page) {
                    return field.eval(page); // The page is all nulls and when you dedupe that it's still all nulls
                }
            };
            default -> throw new IllegalArgumentException("unsupported type [" + elementType + "]");
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
