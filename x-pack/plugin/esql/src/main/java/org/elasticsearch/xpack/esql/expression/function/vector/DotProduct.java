/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

public class DotProduct extends VectorSimilarityFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DotProduct",
        DotProduct::new
    );

    public static final DenseVectorFieldMapper.SimilarityFunction SIMILARITY_FUNCTION = new DenseVectorFieldMapper.SimilarityFunction() {
        @Override
        public float calculateSimilarity(byte[] leftVector, byte[] rightVector) {
            return VectorUtil.dotProduct(leftVector, rightVector);
        }

        @Override
        public float calculateSimilarity(float[] leftVector, float[] rightVector) {
            return VectorUtil.dotProduct(leftVector, rightVector);
        }

        @Override
        public BlockLoaderFunctionConfig.Function function() {
            return BlockLoaderFunctionConfig.Function.V_DOT_PRODUCT;
        }

        @Override
        public String toString() {
            return "V_DOT_PRODUCT";
        }
    };

    @FunctionInfo(
        returnType = "double",
        preview = true,
        description = "Calculates the dot product between two dense_vectors.",
        examples = { @Example(file = "vector-dot-product", tag = "vector-dot-product") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) }
    )
    public DotProduct(
        Source source,
        @Param(
            name = "left",
            type = { "dense_vector" },
            description = "first dense_vector to calculate dot product similarity"
        ) Expression left,
        @Param(
            name = "right",
            type = { "dense_vector" },
            description = "second dense_vector to calculate dot product similarity"
        ) Expression right
    ) {
        super(source, left, right);
    }

    private DotProduct(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new DotProduct(source(), newLeft, newRight);
    }

    @Override
    public DenseVectorFieldMapper.SimilarityFunction getSimilarityFunction() {
        return SIMILARITY_FUNCTION;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DotProduct::new, left(), right());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
