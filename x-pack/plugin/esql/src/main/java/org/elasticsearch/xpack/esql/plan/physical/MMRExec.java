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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.MMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class MMRExec extends UnaryExec {
    private final Attribute diversifyField;
    private final Expression limit;
    private final Expression queryVectorExpression;
    private final Expression options;

    private final VectorData queryVector;
    private final Float lambda;

    public MMRExec(
        Source source,
        PhysicalPlan child,
        Attribute diversifyField,
        Expression limit,
        @Nullable Expression queryVectorExpression,
        @Nullable Expression options
    ) {
        super(source, child);
        this.diversifyField = diversifyField;
        this.limit = limit;
        this.queryVectorExpression = queryVectorExpression;
        this.options = options;

        this.queryVector = extractQueryVectorData(queryVectorExpression);
        this.lambda = MMR.tryExtractLambdaFromOptions(options);
    }

    public Attribute diversifyField() {
        return diversifyField;
    }

    public Expression limit() {
        return limit;
    }

    public VectorData queryVector() {
        return queryVector;
    }

    public Float lambda() {
        return lambda;
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

    public static VectorData extractQueryVectorData(Expression queryVectorExpression) {
        if (queryVectorExpression == null) {
            return null;
        }

        if (queryVectorExpression instanceof Literal literalExpression && queryVectorExpression.dataType() == DataType.DENSE_VECTOR) {
            ArrayList<?> litValues = (ArrayList<?>) literalExpression.value();
            if (litValues == null || litValues.isEmpty()) {
                return null;
            }

            float[] values = new float[litValues.size()];
            for (int i = 0; i < values.length; i++) {
                if (litValues.get(i) instanceof Number numberValue) {
                    values[i] = numberValue.floatValue();
                } else {
                    // should never happen here
                    return null;
                }
            }
            return new VectorData(values);
        }

        return null;
    }
}
