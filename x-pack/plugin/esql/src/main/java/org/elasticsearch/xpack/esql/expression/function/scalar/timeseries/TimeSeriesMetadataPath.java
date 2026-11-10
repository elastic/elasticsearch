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
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

/** Reads a label; an absent label returns null. */
public final class TimeSeriesMetadataPath extends AbstractTimeSeriesMetadataFunction implements AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesMetadataPath",
        TimeSeriesMetadataPath::new
    );

    public TimeSeriesMetadataPath(Source source, Expression labelContainer, Expression label) {
        super(source, List.of(labelContainer, label));
    }

    private TimeSeriesMetadataPath(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return TimeSeriesMetadataFunctionEvaluator.factory(TimeSeriesMetadataFunctionEvaluator.Operation.GET, children(), toEvaluator);
    }

    @Override
    protected TypeResolution resolveArguments() {
        return labelName(1);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public TimeSeriesMetadataPath replaceChildren(List<Expression> children) {
        return new TimeSeriesMetadataPath(source(), children.get(0), children.get(1));
    }

    @Override
    protected NodeInfo<TimeSeriesMetadataPath> info() {
        return NodeInfo.create(this, TimeSeriesMetadataPath::new, children().get(0), children().get(1));
    }
}
