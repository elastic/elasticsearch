/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Keeps only the requested labels, or with {@code exclude} every label but them. Naming an absent label changes nothing.
 */
public final class TimeSeriesMetadataFilter extends AbstractTimeSeriesMetadataFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesMetadataFilter",
        TimeSeriesMetadataFilter::new
    );

    private final boolean exclude;

    public TimeSeriesMetadataFilter(Source source, Expression labelContainer, List<Expression> labels, boolean exclude) {
        super(source, Stream.concat(Stream.of(labelContainer), labels.stream()).toList());
        this.exclude = exclude;
    }

    private TimeSeriesMetadataFilter(StreamInput in) throws IOException {
        super(in);
        this.exclude = in.readBoolean();
    }

    @Override
    protected void writeArguments(StreamOutput out) throws IOException {
        out.writeBoolean(exclude);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /** Whether the requested labels are the ones removed rather than the ones kept. */
    public boolean exclude() {
        return exclude;
    }

    /** The requested label names. */
    public List<Expression> labels() {
        return children().subList(1, children().size());
    }

    @Override
    public DataType dataType() {
        return DataType.TSMD_DATA_TYPE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var operation = exclude ? TimeSeriesMetadataFunctionEvaluator.Operation.DROP : TimeSeriesMetadataFunctionEvaluator.Operation.KEEP;
        return TimeSeriesMetadataFunctionEvaluator.factory(operation, children(), toEvaluator);
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
    public TimeSeriesMetadataFilter replaceChildren(List<Expression> children) {
        return new TimeSeriesMetadataFilter(source(), children.getFirst(), children.subList(1, children.size()), exclude);
    }

    @Override
    protected NodeInfo<TimeSeriesMetadataFilter> info() {
        return NodeInfo.create(this, TimeSeriesMetadataFilter::new, labelContainer(), labels(), exclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), exclude);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && exclude == ((TimeSeriesMetadataFilter) obj).exclude;
    }
}
