/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;
import java.util.Objects;

public class InnerAggregate extends AggregateFunction {

    private final AggregateFunction inner;
    private final CompoundNumericAggregate outer;
    private final String innerName;
    // used when the result needs to be extracted from a map (like in MatrixAggs or Percentiles)
    private final Expression innerKey;

    public InnerAggregate(AggregateFunction inner, CompoundNumericAggregate outer) {
        this(inner.source(), inner, outer, null);
    }

    public InnerAggregate(Source source, AggregateFunction inner, CompoundNumericAggregate outer, Expression innerKey) {
        super(source, outer.field(), outer.arguments());
        this.inner = inner;
        this.outer = outer;
        this.innerName = ((EnclosedAgg) inner).innerName();
        this.innerKey = innerKey;
    }

    @Override
    protected NodeInfo<InnerAggregate> info() {
        return NodeInfo.create(this, InnerAggregate::new, inner, outer, innerKey);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        /* I can't figure out how rewriting this one's children ever worked because its
         * are all twisted up in `outer`. Refusing to rewrite it doesn't break anything
         * that I can see right now so lets just go with it and hope for the best.
         * Maybe someone will make this make sense one day! */
        throw new UnsupportedOperationException("can't be rewritten");
    }

    public AggregateFunction inner() {
        return inner;
    }

    public CompoundNumericAggregate outer() {
        return outer;
    }

    public String innerName() {
        return innerName;
    }

    public Expression innerKey() {
        return innerKey;
    }

    @Override
    public DataType dataType() {
        return inner.dataType();
    }

    @Override
    public String functionName() {
        return inner.functionName();
    }

    @Override
    public int hashCode() {
        return Objects.hash(inner, outer, innerKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == true) {
            InnerAggregate other = (InnerAggregate) obj;
            return Objects.equals(inner, other.inner)
                    && Objects.equals(outer, other.outer)
                    && Objects.equals(innerKey, other.innerKey);
        }
        return false;
    }

    @Override
    public String toString() {
        return nodeName() + "[" + outer + ">" + inner.nodeName() + "]";
    }
}