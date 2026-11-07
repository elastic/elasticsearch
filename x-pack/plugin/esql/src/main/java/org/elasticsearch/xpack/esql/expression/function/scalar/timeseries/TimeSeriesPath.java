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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;

/** Reads a label; an absent label returns null. */
public final class TimeSeriesPath extends AbstractTimeSeriesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesPath",
        TimeSeriesPath::new
    );

    public TimeSeriesPath(Source source, Expression metadata, Expression label) {
        super(source, List.of(metadata, label));
    }

    private TimeSeriesPath(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Reads metadata in a label's native type. Named columns and typed nulls pass through unchanged so aggregation
     * analysis can still see their attributes; metadata reads use TimeSeriesPath followed by an ordinary scalar conversion.
     */
    public static Expression create(Source source, Expression input, String label, DataType resultType, Configuration configuration) {
        if (input.dataType() != DataType.TSMD_DATA_TYPE) {
            if (input.dataType() != resultType) {
                throw new IllegalArgumentException("Conflicting types for label [" + label + "]");
            }
            return input;
        }
        if (label == null || label.isEmpty()) {
            throw new IllegalArgumentException("Label names must be non-empty");
        }
        var value = new TimeSeriesPath(source, input, Literal.keyword(source, label));
        if (resultType == DataType.KEYWORD) {
            return value;
        }
        var conversion = EsqlDataTypeConverter.converterFunctionFactory(resultType);
        if (conversion == null) {
            throw new IllegalArgumentException("Unsupported label type [" + resultType + "]");
        }
        return conversion.apply(source, value, configuration);
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
        return TimeSeriesEvaluator.factory(TimeSeriesEvaluator.Operation.GET, children(), toEvaluator);
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
    public TimeSeriesPath replaceChildren(List<Expression> children) {
        return new TimeSeriesPath(source(), children.get(0), children.get(1));
    }

    @Override
    protected NodeInfo<TimeSeriesPath> info() {
        return NodeInfo.create(this, TimeSeriesPath::new, children().get(0), children().get(1));
    }
}
