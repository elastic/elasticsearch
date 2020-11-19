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
import java.util.Objects;

public class Percentiles extends CompoundNumericAggregate {

    private final List<Expression> percents;
    private final PercentilesConfig percentilesConfig;

    public Percentiles(Source source, Expression field, List<Expression> percents, PercentilesConfig percentilesConfig) {
        super(source, field, percents);
        this.percents = percents;
        this.percentilesConfig = percentilesConfig;
    }

    @Override
    protected NodeInfo<Percentiles> info() {
        return NodeInfo.create(this, Percentiles::new, field(), percents, percentilesConfig);
    }

    @Override
    public Percentiles replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new Percentiles(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()), percentilesConfig);
    }

    public List<Expression> percents() {
        return percents;
    }
    
    public PercentilesConfig percentilesConfig() {
        return percentilesConfig;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), percentilesConfig);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        return Objects.equals(percentilesConfig, ((Percentiles) o).percentilesConfig);
    }
}
