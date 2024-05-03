/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.util.function.Supplier;

public class ColumnExtractOperator extends AbstractPageMappingOperator {

    public record Factory(
        ElementType[] types,
        ExpressionEvaluator.Factory inputEvalSupplier,
        Supplier<ColumnExtractOperator.Evaluator> evaluatorSupplier
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ColumnExtractOperator(types, inputEvalSupplier.get(driverContext), evaluatorSupplier.get(), driverContext);
        }

        @Override
        public String describe() {
            return "ColumnExtractOperator[evaluator=" + evaluatorSupplier.get() + "]";
        }
    }

    private final ElementType[] types;
    private final EvalOperator.ExpressionEvaluator inputEvaluator;
    private final ColumnExtractOperator.Evaluator evaluator;
    private final DriverContext driverContext;

    public ColumnExtractOperator(
        ElementType[] types,
        ExpressionEvaluator inputEvaluator,
        Evaluator evaluator,
        DriverContext driverContext
    ) {
        this.types = types;
        this.inputEvaluator = inputEvaluator;
        this.evaluator = evaluator;
        this.driverContext = driverContext;
    }

    @Override
    protected Page process(Page page) {
        int rowsCount = page.getPositionCount();

        Block.Builder[] blockBuilders = new Block.Builder[types.length];
        try {
            for (int i = 0; i < types.length; i++) {
                blockBuilders[i] = types[i].newBlockBuilder(rowsCount, driverContext.blockFactory());
            }

            try (BytesRefBlock input = (BytesRefBlock) inputEvaluator.eval(page)) {
                BytesRef spare = new BytesRef();
                for (int row = 0; row < rowsCount; row++) {
                    if (input.isNull(row)) {
                        for (int i = 0; i < blockBuilders.length; i++) {
                            blockBuilders[i].appendNull();
                        }
                        continue;
                    }
                    evaluator.computeRow(input, row, blockBuilders, spare);
                }

                Block[] blocks = new Block[blockBuilders.length];
                for (int i = 0; i < blockBuilders.length; i++) {
                    blocks[i] = blockBuilders[i].build();
                }

                return page.appendBlocks(blocks);
            }
        } finally {
            Releasables.closeExpectNoException(blockBuilders);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("evaluator=");
        sb.append(evaluator.toString());
        sb.append("]");
        return sb.toString();
    }

    public interface Evaluator {
        void computeRow(BytesRefBlock input, int row, Block.Builder[] target, BytesRef spare);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(inputEvaluator, super::close);
    }
}
