/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

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

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;

public class CosineSimilarity extends VectorSimilarityFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CosineSimilarity",
        CosineSimilarity::new
    );
    static final SimilarityEvaluatorFunction SIMILARITY_FUNCTION = COSINE::compare;

    @FunctionInfo(
        returnType = "double",
        preview = true,
        description = "Calculates the cosine similarity between two dense_vectors.",
        examples = { @Example(file = "vector-cosine-similarity", tag = "vector-cosine-similarity") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) }
    )
    public CosineSimilarity(
        Source source,
        @Param(name = "left", type = { "dense_vector" }, description = "first dense_vector to calculate cosine similarity") Expression left,
        @Param(
            name = "right",
            type = { "dense_vector" },
            description = "second dense_vector to calculate cosine similarity"
        ) Expression right
    ) {
        super(source, left, right);
    }

    private CosineSimilarity(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new CosineSimilarity(source(), newLeft, newRight);
    }

    @Override
    protected SimilarityEvaluatorFunction getSimilarityFunction() {
        return SIMILARITY_FUNCTION;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CosineSimilarity::new, left(), right());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
