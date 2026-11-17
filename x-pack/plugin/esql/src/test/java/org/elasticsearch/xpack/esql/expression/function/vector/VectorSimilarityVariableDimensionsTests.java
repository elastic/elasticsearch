/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import static org.hamcrest.Matchers.equalTo;

/**
 * Vectors produced by an expression, such as a list read from an external file, can differ in length from row to row
 * within one {@link Page}. Each row must be evaluated against its own length.
 */
public class VectorSimilarityVariableDimensionsTests extends ESTestCase {
    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testRowsWithDifferentDimensions() {
        float[][] left = { { 1f, 2f }, { 3f, 4f, 5f }, { 6f } };
        float[][] right = { { 2f, 1f }, { 5f, 4f, 3f }, { 9f } };

        Expression leftAttr = new ReferenceAttribute(Source.EMPTY, "left", DataType.DENSE_VECTOR);
        Expression rightAttr = new ReferenceAttribute(Source.EMPTY, "right", DataType.DENSE_VECTOR);
        DotProduct dotProduct = new DotProduct(Source.EMPTY, leftAttr, rightAttr);

        EvaluatorMapper.ToEvaluator toEvaluator = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                int channel = expression == leftAttr ? 0 : 1;
                return context -> new ExpressionEvaluator() {
                    @Override
                    public Block eval(Page page) {
                        Block block = page.getBlock(channel);
                        block.incRef();
                        return block;
                    }

                    @Override
                    public long baseRamBytesUsed() {
                        return 0;
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };

        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        try (
            ExpressionEvaluator evaluator = dotProduct.toEvaluator(toEvaluator).get(driverContext);
            Page page = new Page(vectors(left), vectors(right));
            DoubleBlock result = (DoubleBlock) evaluator.eval(page)
        ) {
            assertThat(result.getPositionCount(), equalTo(left.length));
            for (int row = 0; row < left.length; row++) {
                double expected = DotProduct.SIMILARITY_FUNCTION.calculateSimilarity(left[row], right[row]);
                assertThat("row " + row, result.getDouble(result.getFirstValueIndex(row)), equalTo(expected));
            }
        }
    }

    private FloatBlock vectors(float[][] rows) {
        try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder(rows.length)) {
            for (float[] row : rows) {
                builder.beginPositionEntry();
                for (float value : row) {
                    builder.appendFloat(value);
                }
                builder.endPositionEntry();
            }
            return builder.build();
        }
    }
}
