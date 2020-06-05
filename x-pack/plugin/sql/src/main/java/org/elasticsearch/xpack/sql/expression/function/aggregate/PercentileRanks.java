/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class PercentileRanks extends CompoundNumericAggregate {

    private final List<Expression> values;

    public PercentileRanks(Source source, Expression field, List<Expression> values) {
        super(source, field, values);
        this.values = values;
    }

    @Override
    protected NodeInfo<PercentileRanks> info() {
        return NodeInfo.create(this, PercentileRanks::new, field(), values);
    }

    @Override
    public PercentileRanks replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new PercentileRanks(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    public List<Expression> values() {
        return values;
    }
}
