/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;

import java.util.List;

/**
 * An implicit default time-series aggregation function that delegates to the appropriate
 * time-series aggregation function based on the field's data type.
 * <p>
 * The purpose of this class is to provide a better type resolution error message for implicit time-series aggregations.
 * It avoids having to change the source text of the field expression, which would be more invasive.
 */
public class DefaultTimeSeriesAggregateFunction extends TimeSeriesAggregateFunction implements SurrogateExpression {

    private final TimeSeriesAggregateFunction delegate;
    private final Expression timestamp;

    public DefaultTimeSeriesAggregateFunction(Expression field, Expression timestamp) {
        this(field.source(), field, Literal.TRUE, AggregateFunction.NO_WINDOW, timestamp);
    }

    public DefaultTimeSeriesAggregateFunction(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
        // the delegate is not propagated as a parameter, making the delegate a child expression
        // otherwise our resolveType would not be called as child expressions are resolved first
        if (field.typeResolved().resolved() && field.dataType().isHistogram()) {
            this.delegate = new HistogramMergeOverTime(source, field, filter, window);
        } else {
            this.delegate = new LastOverTime(source, field, filter, window, timestamp);
        }
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return delegate.perTimeSeriesAggregation();
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        if (filter == filter()) {
            return this;
        }
        return new DefaultTimeSeriesAggregateFunction(source(), field(), filter, window(), timestamp);
    }

    @Override
    public DataType dataType() {
        return delegate.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DefaultTimeSeriesAggregateFunction(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3)
        );
    }

    @Override
    protected NodeInfo<DefaultTimeSeriesAggregateFunction> info() {
        return NodeInfo.create(this, DefaultTimeSeriesAggregateFunction::new, field(), filter(), window(), timestamp);
    }

    @Override
    public String getWriteableName() {
        return delegate.getWriteableName();
    }

    @Override
    public TypeResolution resolveType() {
        TypeResolution typeResolution = delegate.resolveType();
        if (typeResolution.unresolved()) {
            String message = typeResolution.message();
            int pos = message.indexOf(field().sourceText());
            if (pos >= 0) {
                message = message.substring(0, pos)
                    + "implicit time-series aggregation function ("
                    + delegate.getClass().getSimpleName()
                    + ") for "
                    + message.substring(pos);
            }
            return new TypeResolution(message + "; to aggregate non-numeric fields, use the FROM command instead of the TS command");
        }
        return typeResolution;
    }

    @Override
    public Expression surrogate() {
        return delegate;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean childrenResolved() {
        return super.childrenResolved();
    }
}
