/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;

import java.io.IOException;
import java.util.Objects;

public class MMRExec extends UnaryExec implements ExecutesOn.Coordinator {
    private final Attribute diversifyField;
    private final Expression limit;
    private final Expression queryVector;
    private final Expression options;

    public MMRExec(
        Source source,
        PhysicalPlan child,
        Attribute diversifyField,
        Expression limit,
        @Nullable Expression queryVector,
        @Nullable Expression options
    ) {
        super(source, child);
        this.diversifyField = diversifyField;
        this.limit = limit;
        this.queryVector = queryVector;
        this.options = options;
    }

    public Attribute diversifyField() {
        return diversifyField;
    }

    public Expression limit() {
        return limit;
    }

    public Expression queryVector() {
        return queryVector;
    }

    public Expression options() {
        return options;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new MMRExec(source(), newChild, diversifyField, limit, queryVector, options);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, MMRExec::new, child(), diversifyField, limit, queryVector, options);
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
        return Objects.equals(this.diversifyField, mmr.diversifyField)
            && Objects.equals(this.limit, mmr.limit)
            && Objects.equals(this.queryVector, mmr.queryVector)
            && Objects.equals(this.options, mmr.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), diversifyField, limit, queryVector, options);
    }
}
