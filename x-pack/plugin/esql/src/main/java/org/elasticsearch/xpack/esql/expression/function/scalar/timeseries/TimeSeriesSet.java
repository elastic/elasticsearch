/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

/** Replaces a label using each row's value; empty or null values remove the label. */
public final class TimeSeriesSet extends TimeSeriesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesSet",
        TimeSeriesSet::new
    );

    public TimeSeriesSet(Source source, Expression metadata, Expression label, Expression value) {
        super(source, List.of(metadata, label, value));
    }

    private TimeSeriesSet(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.TIMESERIES_DATA_TYPE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("time-series metadata evaluation is not implemented yet");
    }

    @Override
    protected TypeResolution resolveArguments() {
        return labelName(1).and(labelValue(2));
    }

    @Override
    public TimeSeriesSet replaceChildren(List<Expression> children) {
        return new TimeSeriesSet(source(), children.get(0), children.get(1), children.get(2));
    }

    @Override
    protected NodeInfo<TimeSeriesSet> info() {
        return NodeInfo.create(this, TimeSeriesSet::new, children().get(0), children().get(1), children().get(2));
    }
}
