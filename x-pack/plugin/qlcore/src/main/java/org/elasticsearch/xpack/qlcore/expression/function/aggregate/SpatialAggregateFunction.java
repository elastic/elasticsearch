/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.qlcore.expression.function.aggregate;

import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.Source;

import java.util.Objects;

/**
 * All spatial aggregate functions extend this class to enable the planning of reading from doc values for higher performance.
 * The AggregateMapper class will generate multiple aggregation functions for each combination, allowing the planner to
 * select the best one.
 */
public abstract class SpatialAggregateFunction extends AggregateFunction {
    protected final boolean useDocValues;

    protected SpatialAggregateFunction(Source source, Expression field, boolean useDocValues) {
        super(source, field);
        this.useDocValues = useDocValues;
    }

    public abstract SpatialAggregateFunction withDocValues();

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), useDocValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialAggregateFunction other = (SpatialAggregateFunction) obj;
            return Objects.equals(other.field(), field())
                && Objects.equals(other.parameters(), parameters())
                && Objects.equals(other.useDocValues, useDocValues);
        }
        return false;
    }

    public boolean useDocValues() {
        return useDocValues;
    }
}
