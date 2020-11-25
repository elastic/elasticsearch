/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new PercentileRanks(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()), percentilesConfig);
    }
    
    @SuppressWarnings("unchecked")
    public List<Expression> values() {
        return (List<Expression>) parameters();
    }

}
