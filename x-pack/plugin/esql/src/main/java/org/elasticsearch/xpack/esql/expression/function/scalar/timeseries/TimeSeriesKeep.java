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
import java.util.stream.Stream;

/** Keeps only the requested labels; keeping an absent label adds nothing. */
public final class TimeSeriesKeep extends AbstractTimeSeriesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesKeep",
        TimeSeriesKeep::new
    );

    public TimeSeriesKeep(Source source, Expression labelContainer, List<Expression> labels) {
        super(source, Stream.concat(Stream.of(labelContainer), labels.stream()).toList());
    }

    private TimeSeriesKeep(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.TSMD_DATA_TYPE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return TimeSeriesEvaluator.factory(TimeSeriesEvaluator.Operation.KEEP, children(), toEvaluator);
    }

    @Override
    protected TypeResolution resolveArguments() {
        for (int i = 1; i < children().size(); i++) {
            TypeResolution resolution = labelName(i);
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public TimeSeriesKeep replaceChildren(List<Expression> children) {
        return new TimeSeriesKeep(source(), children.getFirst(), children.subList(1, children.size()));
    }

    @Override
    protected NodeInfo<TimeSeriesKeep> info() {
        return NodeInfo.create(this, TimeSeriesKeep::new, labelContainer(), children().subList(1, children().size()));
    }
}
