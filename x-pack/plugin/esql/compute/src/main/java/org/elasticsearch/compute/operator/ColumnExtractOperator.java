/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.function.Supplier;

@Experimental
public class ColumnExtractOperator extends AbstractPageMappingOperator {

    public record Factory(
        ElementType[] types,
        Supplier<EvalOperator.ExpressionEvaluator> inputEvalSupplier,
        Supplier<ColumnExtractOperator.Evaluator> evaluatorSupplier
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new ColumnExtractOperator(types, inputEvalSupplier.get(), evaluatorSupplier.get());
        }

        @Override
        public String describe() {
            return "ColumnExtractOperator[evaluator=" + evaluatorSupplier.get() + "]";
        }
    }

    private final ElementType[] types;
    private final EvalOperator.ExpressionEvaluator inputEvaluator;
    private final ColumnExtractOperator.Evaluator evaluator;

    public ColumnExtractOperator(
        ElementType[] types,
        EvalOperator.ExpressionEvaluator inputEvaluator,
        ColumnExtractOperator.Evaluator evaluator
    ) {
        this.types = types;
        this.inputEvaluator = inputEvaluator;
        this.evaluator = evaluator;
    }

    @Override
    protected Page process(Page page) {
        int rowsCount = page.getPositionCount();

        Block.Builder[] blockBuilders = new Block.Builder[types.length];
        for (int i = 0; i < types.length; i++) {
            blockBuilders[i] = types[i].newBlockBuilder(rowsCount);
        }

        BytesRefBlock input = (BytesRefBlock) inputEvaluator.eval(page);
        BytesRef spare = new BytesRef();
        for (int row = 0; row < rowsCount; row++) {
            if (input.isNull(row)) {
                for (int i = 0; i < blockBuilders.length; i++) {
                    blockBuilders[i].appendNull();
                }
                continue;
            }

            // For now more than a single input value will just read the first one
            int position = input.getFirstValueIndex(row);
            evaluator.computeRow(input.getBytesRef(position, spare), blockBuilders);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return page.appendBlocks(blocks);
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
        void computeRow(BytesRef input, Block.Builder[] target);
    }

}
