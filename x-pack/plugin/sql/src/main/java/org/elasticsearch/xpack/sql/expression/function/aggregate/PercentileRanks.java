/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class PercentileRanks extends PercentileCompoundAggregate {

    public PercentileRanks(Source source, Expression field, List<Expression> values, PercentilesConfig percentilesConfig) {
        super(source, field, values, percentilesConfig);
    }

    @Override
    protected NodeInfo<PercentileRanks> info() {
        return NodeInfo.create(this, PercentileRanks::new, field(), values(), percentilesConfig);
    }

    @Override
    public PercentileRanks replaceChildren(List<Expression> newChildren) {
        return new PercentileRanks(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()), percentilesConfig);
    }

    @SuppressWarnings("unchecked")
    public List<Expression> values() {
        return (List<Expression>) parameters();
    }

}
