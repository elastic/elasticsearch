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

public class Percentiles extends PercentileCompoundAggregate {

    public Percentiles(Source source, Expression field, List<Expression> percents, PercentilesConfig percentilesConfig) {
        super(source, field, percents, percentilesConfig);
    }

    @Override
    protected NodeInfo<Percentiles> info() {
        return NodeInfo.create(this, Percentiles::new, field(), percents(), percentilesConfig());
    }

    @Override
    public Percentiles replaceChildren(List<Expression> newChildren) {
        return new Percentiles(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()), percentilesConfig());
    }

    @SuppressWarnings("unchecked")
    public List<Expression> percents() {
        return (List<Expression>) parameters();
    }

}
