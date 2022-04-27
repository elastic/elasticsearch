/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public abstract class PercentileCompoundAggregate extends CompoundNumericAggregate {
    protected final PercentilesConfig percentilesConfig;

    public PercentileCompoundAggregate(Source source, Expression field, List<Expression> arguments, PercentilesConfig percentilesConfig) {
        super(source, field, arguments);
        this.percentilesConfig = percentilesConfig;
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
        if (super.equals(o) == false) {
            return false;
        }

        return Objects.equals(percentilesConfig, ((Percentiles) o).percentilesConfig);
    }
}
