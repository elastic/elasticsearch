/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.EnclosedAgg;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

/**
 * Find the arithmatic mean of a field.
 */
public class Avg extends NumericAggregate implements EnclosedAgg {

    public Avg(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Avg> info() {
        return NodeInfo.create(this, Avg::new, field());
    }

    @Override
    public Avg replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Avg(source(), newChildren.get(0));
    }

    @Override
    public String innerName() {
        return "avg";
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }
}
