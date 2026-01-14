/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

public class MMR extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "MMR", MMR::new);

    private final Attribute diversifyField;
    private final Attribute queryVector;
    private final Attribute lambdaValue;
    private final Expression limit;

    public MMR(
        Source source,
        LogicalPlan child,
        Attribute diversifyField,
        Expression limit,
        @Nullable Attribute queryVector,
        @Nullable Attribute lambdaValue
    ) {
        super(source, child);
        this.diversifyField = diversifyField;
        this.limit = limit;
        this.queryVector = queryVector;
        this.lambdaValue = lambdaValue;
    }

    public MMR(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class));
        this.diversifyField = in.readNamedWriteable(Attribute.class);
        this.limit = in.readNamedWriteable(Expression.class);
        this.queryVector = in.readOptionalNamedWriteable(Attribute.class);
        this.lambdaValue = in.readOptionalNamedWriteable(Attribute.class);
    }

    public Attribute diversifyField() {
        return diversifyField;
    }

    public Expression limit() {
        return limit;
    }

    public Attribute queryVector() {
        return queryVector;
    }

    public Attribute lambdaValue() {
        return lambdaValue;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return null;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return null;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(this.diversifyField);
        out.writeNamedWriteable(this.limit);
        if (this.queryVector != null) {
            out.writeOptionalNamedWriteable(queryVector);
        }
        if (this.lambdaValue != null) {
            out.writeOptionalNamedWriteable(lambdaValue);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MMR mmr = (MMR) o;
        return Objects.equals(this.diversifyField, mmr.diversifyField)
            && Objects.equals(this.limit, mmr.limit)
            && Objects.equals(this.queryVector, mmr.queryVector)
            && Objects.equals(this.lambdaValue, mmr.lambdaValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), diversifyField, limit, queryVector, lambdaValue);
    }
}
