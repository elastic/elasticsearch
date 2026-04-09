/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.MMR;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan for the MMR command.
 * MMR performs result diversification on incoming results using maximum marginal relevance.
 * The input is a set of limited rows, where at least one field is a dense vector to use for vector comparison.
 * The output is a reduced set of results, in the same order as the input, but "diversified" to be results that are semantically
 * diverse from each other within the input set.
 */
public class MMRExec extends UnaryExec {
    private final Attribute diversifyField;
    private final Expression limit;
    private final Expression queryVectorExpression;
    private final MapExpression options;

    public MMRExec(
        Source source,
        PhysicalPlan child,
        Attribute diversifyField,
        Expression limit,
        @Nullable Expression queryVectorExpression,
        @Nullable MapExpression options
    ) {
        super(source, child);
        this.diversifyField = diversifyField;
        this.limit = limit;
        this.queryVectorExpression = queryVectorExpression;
        this.options = options;
    }

    public Attribute diversifyField() {
        return diversifyField;
    }

    public Expression limit() {
        return limit;
    }

    public MapExpression options() {
        return options;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new MMRExec(source(), newChild, diversifyField, limit, queryVectorExpression, options);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, MMRExec::new, child(), diversifyField, limit, queryVectorExpression, options);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        MMRExec mmr = (MMRExec) o;
        return Objects.equals(this.child(), mmr.child())
            && Objects.equals(this.diversifyField, mmr.diversifyField)
            && Objects.equals(this.limit, mmr.limit)
            && Objects.equals(this.queryVectorExpression, mmr.queryVectorExpression)
            && Objects.equals(this.options, mmr.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), diversifyField, limit, queryVectorExpression, options);
    }

    public VectorData queryVector() {
        if (queryVectorExpression == null) {
            return null;
        }

        assert queryVectorExpression instanceof Literal && queryVectorExpression.dataType() == DataType.DENSE_VECTOR
            : "query vector must be resolved to a dense_vector literal";

        Literal literal = (Literal) queryVectorExpression;
        @SuppressWarnings("unchecked")
        List<Number> litValues = literal.value() instanceof Number ? List.of((Number) literal.value()) : (List<Number>) literal.value();

        float[] values = new float[litValues.size()];
        for (int i = 0; i < litValues.size(); i++) {
            values[i] = litValues.get(i).floatValue();
        }
        return VectorData.fromFloats(values);
    }

    public float lambda() {
        if (options == null) {
            return MMR.DEFAULT_LAMBDA;
        }

        Literal lambdaValueExpression = (Literal) options.keyFoldedMap().getOrDefault(MMR.LAMBDA_OPTION_NAME, null);
        if (lambdaValueExpression == null) {
            return MMR.DEFAULT_LAMBDA;
        }
        return ((Number) lambdaValueExpression.value()).floatValue();
    }

    public int limitValue() {
        assert limit != null && limit instanceof Literal && ((Literal) limit).value() instanceof Number : "limit must be a number literal";
        return ((Number) ((Literal) limit).value()).intValue();
    }
}
