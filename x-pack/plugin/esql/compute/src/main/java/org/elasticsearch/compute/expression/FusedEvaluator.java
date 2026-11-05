/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

/**
 * Evaluates a whole tree of {@link FusableEvaluatorFactory fusable} expressions, such as {@code (a + b) * c},
 * in one loop over the page instead of materializing a block for every intermediate result.
 * <p>
 * The per-page work that doesn't depend on the tree lives here: evaluating the leaf inputs, building the
 * result block and lazily creating one {@link Warnings} per fused node. The per-position work, which is a
 * straight-line sequence of null checks, multi-value checks and kernel calls, is spun into a hidden
 * subclass per {@link FusedShape shape} by {@link FusedEvaluatorSpinner}, so HotSpot sees the whole
 * expression as one method and can keep the intermediate values in registers.
 * <p>
 * A fused tree reproduces what the unfused tree of generated evaluators would have done for every
 * position: nodes are evaluated inner-first, a {@code null} input makes the node {@code null} without
 * looking at its later inputs, a multi-valued input warns on the node consuming it, and a kernel throwing
 * one of its {@link EvaluatorKernel#warnExceptions()} warns and yields {@code null}. The only visible
 * difference is that warnings of different nodes may reach the driver's sink in a different order.
 */
public abstract class FusedEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FusedEvaluator.class);

    private static final String MULTI_VALUE_MESSAGE = "single-value function encountered multi-value";

    private final DriverContext driverContext;
    private final ExpressionEvaluator[] leaves;
    private final WarningSourceLocation[] sources;
    private final Warnings[] warnings;
    private final ElementType resultType;
    private final String description;

    /**
     * Called by the spun subclass. The leaf evaluators are owned and closed by this instance.
     *
     * @param sources one warning location per fused node, in evaluation order
     * @param description what {@link #toString()} returns, the unfused tree's description
     */
    protected FusedEvaluator(
        DriverContext driverContext,
        ExpressionEvaluator[] leaves,
        WarningSourceLocation[] sources,
        ElementType resultType,
        String description
    ) {
        this.driverContext = driverContext;
        this.leaves = leaves;
        this.sources = sources;
        this.warnings = new Warnings[sources.length];
        this.resultType = resultType;
        this.description = description;
    }

    @Override
    public final Block eval(Page page) {
        Block[] blocks = new Block[leaves.length];
        try {
            for (int i = 0; i < leaves.length; i++) {
                blocks[i] = leaves[i].eval(page);
            }
            int positionCount = page.getPositionCount();
            try (Block.Builder result = resultType.newBlockBuilder(positionCount, driverContext.blockFactory())) {
                eval(positionCount, blocks, result);
                return result.build();
            }
        } finally {
            Releasables.closeExpectNoException(blocks);
        }
    }

    /**
     * Appends the fused result of every position to {@code result}. Implemented by the spun subclass;
     * {@code blocks} holds one block per leaf and {@code result} is a builder for the root kernel's type.
     */
    protected abstract void eval(int positionCount, Block[] blocks, Block.Builder result);

    /**
     * Called by the spun subclass when node {@code node} reads a multi-valued input.
     */
    protected final void warnMultiValue(int node) {
        warnings(node).registerException(IllegalArgumentException.class, MULTI_VALUE_MESSAGE);
    }

    /**
     * Called by the spun subclass when node {@code node}'s kernel throws one of its warn exceptions.
     */
    protected final void warnException(int node, Exception e) {
        warnings(node).registerException(e);
    }

    private Warnings warnings(int node) {
        Warnings w = warnings[node];
        if (w == null) {
            w = driverContext.createWarnings(sources[node]);
            warnings[node] = w;
        }
        return w;
    }

    @Override
    public long baseRamBytesUsed() {
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(leaves) + RamUsageEstimator.shallowSizeOf(sources)
            + RamUsageEstimator.shallowSizeOf(warnings);
        for (ExpressionEvaluator leaf : leaves) {
            bytes += leaf.baseRamBytesUsed();
        }
        return bytes;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(leaves);
    }

    @Override
    public String toString() {
        return description;
    }

    /**
     * Builds {@link FusedEvaluator}s for one expression tree. Obtain one through {@link #tryFuse}.
     */
    public static final class Factory implements ExpressionEvaluator.Factory {
        private final ExpressionEvaluator.Factory original;
        private final FusedShape shape;
        private final List<ExpressionEvaluator.Factory> leaves;
        private final WarningSourceLocation[] sources;
        private final Object[] fixedValues;
        private final MethodHandle constructor;

        private Factory(
            ExpressionEvaluator.Factory original,
            FusedShape shape,
            List<ExpressionEvaluator.Factory> leaves,
            WarningSourceLocation[] sources,
            Object[] fixedValues,
            MethodHandle constructor
        ) {
            this.original = original;
            this.shape = shape;
            this.leaves = leaves;
            this.sources = sources;
            this.fixedValues = fixedValues;
            this.constructor = constructor;
        }

        /**
         * Fuses the largest {@link FusableEvaluatorFactory} subtree rooted at {@code root}. Inputs that aren't
         * fusable, such as constants or evaluators over {@code BytesRef}s, stay separate evaluators whose
         * blocks the fused evaluator reads.
         *
         * @return a factory for the fused tree, or empty if {@code root} isn't fusable, if fusing wouldn't
         *         save an intermediate block because the tree has a single kernel, or if the evaluator
         *         class couldn't be spun. Callers use the unfused {@code root} in that case.
         */
        public static Optional<Factory> tryFuse(ExpressionEvaluator.Factory root) {
            FusedShape.Builder builder = new FusedShape.Builder();
            FusedShape shape = builder.build(root);
            if (shape == null || shape.nodes().size() < 2) {
                return Optional.empty();
            }
            return FusedEvaluatorSpinner.SHARED.constructor(shape)
                .map(
                    constructor -> new Factory(
                        root,
                        shape,
                        List.copyOf(builder.leaves()),
                        builder.sources().toArray(WarningSourceLocation[]::new),
                        builder.fixedValues().toArray(),
                        constructor
                    )
                );
        }

        @Override
        public ExpressionEvaluator get(DriverContext context) {
            ExpressionEvaluator[] leafEvaluators = new ExpressionEvaluator[leaves.size()];
            try {
                for (int i = 0; i < leafEvaluators.length; i++) {
                    leafEvaluators[i] = leaves.get(i).get(context);
                }
                return (FusedEvaluator) constructor.invokeExact(
                    context,
                    leafEvaluators,
                    sources,
                    shape.root().resultKind().elementType,
                    original.toString(),
                    fixedValues
                );
            } catch (RuntimeException | Error e) {
                Releasables.closeExpectNoException(leafEvaluators);
                throw e;
            } catch (Throwable t) {
                Releasables.closeExpectNoException(leafEvaluators);
                throw new IllegalStateException("failed to build fused evaluator for [" + original + "]", t);
            }
        }

        @Override
        public boolean eagerEvalSafeInLazy() {
            return original.eagerEvalSafeInLazy();
        }

        /**
         * The leaf factories the fused evaluator reads blocks from, left to right.
         */
        public List<ExpressionEvaluator.Factory> leaves() {
            return leaves;
        }

        FusedShape shape() {
            return shape;
        }

        MethodHandle constructor() {
            return constructor;
        }

        /**
         * The same description as the unfused tree, so plans and tests read the same either way.
         */
        @Override
        public String toString() {
            return original.toString();
        }
    }
}
