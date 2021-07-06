/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

/**
 * Marker for expressions that were derived from an aggregate function.
 *
 * Some optimizations can eliminate aggregate functions but the information that the expression has been an aggregate needs to be retained.
 */
public class EliminatedAggregateFunction extends AggregateFunction {

    public EliminatedAggregateFunction(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        return field().fold();
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public EliminatedAggregateFunction replaceChildren(List<Expression> newChildren) {
        return new EliminatedAggregateFunction(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<EliminatedAggregateFunction> info() {
        return NodeInfo.create(this, EliminatedAggregateFunction::new, field());
    }
}
