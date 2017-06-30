/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;
import org.elasticsearch.xpack.sql.type.DataType;

public class InnerAggregate extends AggregateFunction {

    private final AggregateFunction inner;
    private final CompoundAggregate outer;
    private final String innerId;
    // used when the result needs to be extracted from a map (like in MatrixAggs)
    private final Expression innerKey;

    public InnerAggregate(AggregateFunction inner, CompoundAggregate outer) {
        this(inner, outer, null);
    }

    public InnerAggregate(AggregateFunction inner, CompoundAggregate outer, Expression innerKey) {
        super(inner.location(), outer.argument());
        this.inner = inner;
        this.outer = outer;
        this.innerId = ((EnclosedAgg) inner).innerName();
        this.innerKey = innerKey;
    }

    public AggregateFunction inner() {
        return inner;
    }

    public CompoundAggregate outer() {
        return outer;
    }

    public String innerId() {
        return innerId;
    }

    public Expression innerKey() {
        return innerKey;
    }

    @Override
    public DataType dataType() {
        return inner.dataType();
    }
    
    @Override
    public String functionId() {
        return outer.id().toString();
    }

    @Override
    public AggregateFunctionAttribute toAttribute() {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        return new AggregateFunctionAttribute(location(), name(), dataType(), outer.id(), functionId(), AggPath.metricValue(functionId(), innerId));
    }

    @Override
    public boolean functionEquals(Function f) {
        if (super.equals(f)) {
            InnerAggregate other = (InnerAggregate) f;
            return inner.equals(other.inner) && outer.equals(other.outer);
        }
        return false;
    }

    @Override
    public String name() {
        return "(" + inner.functionName() + "#" + inner.id() + "/" + outer.toString() + ")";
    }
}