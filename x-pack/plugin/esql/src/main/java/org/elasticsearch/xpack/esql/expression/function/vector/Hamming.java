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

public class Hamming extends VectorSimilarityFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Hamming", Hamming::new);
    static final SimilarityEvaluatorFunction SIMILARITY_FUNCTION = Hamming::calculateSimilarity;

    @FunctionInfo(
        returnType = "double",
        preview = true,
        description = "Calculates the Hamming distance between two dense vectors.",
        examples = { @Example(file = "vector-hamming", tag = "vector-hamming") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") }
    )
    public Hamming(
        Source source,
        @Param(
            name = "left",
            type = { "dense_vector" },
            description = "First dense_vector to use to calculate the Hamming distance"
        ) Expression left,
        @Param(
            name = "right",
            type = { "dense_vector" },
            description = "Second dense_vector to use to calculate the Hamming distance"
        ) Expression right
    ) {
        super(source, left, right);
    }

    private Hamming(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected SimilarityEvaluatorFunction getSimilarityFunction() {
        return SIMILARITY_FUNCTION;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new Hamming(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Hamming::new, left(), right());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public static float calculateSimilarity(float[] leftScratch, float[] rightScratch) {
        byte[] a = new byte[leftScratch.length];
        byte[] b = new byte[rightScratch.length];
        for (int i = 0; i < leftScratch.length; i++) {
            a[i] = (byte) leftScratch[i];
        }
        for (int i = 0; i < leftScratch.length; i++) {
            b[i] = (byte) rightScratch[i];
        }
        return VectorUtil.xorBitCount(a, b);
    }
}
