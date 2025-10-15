/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.subquery;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnaryExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.types.PromqlDataTypes;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;

import java.io.IOException;
import java.util.Objects;

public class Subquery extends UnaryExpression {
    private final TimeValue range;
    private final TimeValue resolution;
    private final Evaluation evaluation;

    public Subquery(Source source, Expression query, TimeValue range, TimeValue resolution, Evaluation evaluation) {
        super(source, query);
        this.range = range;
        this.resolution = resolution;
        this.evaluation = evaluation;
    }

    public Expression query() {
        return child();
    }

    public TimeValue range() {
        return range;
    }

    public TimeValue resolution() {
        return resolution;
    }

    public Evaluation evaluation() {
        return evaluation;
    }

    @Override
    public DataType dataType() {
        return PromqlDataTypes.RANGE_VECTOR;
    }

    @Override
    protected NodeInfo<Subquery> info() {
        return NodeInfo.create(this, Subquery::new, child(), range, resolution, evaluation);
    }

    @Override
    protected Subquery replaceChild(Expression newChild) {
        return new Subquery(source(), newChild, range, resolution, evaluation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Subquery subquery = (Subquery) o;
        return Objects.equals(range, subquery.range)
            && Objects.equals(resolution, subquery.resolution)
            && Objects.equals(evaluation, subquery.evaluation)
            && Objects.equals(child(), subquery.child());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), child(), range, resolution, evaluation);
    }

    @Override
    public String getWriteableName() {
        throw new EsqlIllegalArgumentException("should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new EsqlIllegalArgumentException("should not be serialized");
    }
}
