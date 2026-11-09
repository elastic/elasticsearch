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
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

/**
 * Writes one label: the value replaces the label's values, and a missing or empty value removes the label, as PromQL has no
 * empty label. Several values write a multi-valued label. The label keeps its sorted position, so the result stays canonical.
 */
public final class TimeSeriesSet extends AbstractTimeSeriesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesSet",
        TimeSeriesSet::new
    );

    public TimeSeriesSet(Source source, Expression labelContainer, Expression label, Expression value) {
        super(source, List.of(labelContainer, label, value));
    }

    private TimeSeriesSet(StreamInput in) throws IOException {
        super(in);
    }

    public Expression label() {
        return children().get(1);
    }

    public Expression value() {
        return children().get(2);
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
        return TimeSeriesEvaluator.factory(TimeSeriesEvaluator.Operation.SET, children(), toEvaluator);
    }

    @Override
    protected TypeResolution resolveArguments() {
        return labelName(1).and(TypeResolutions.isString(value(), sourceText(), TypeResolutions.ParamOrdinal.THIRD));
    }

    @Override
    public TimeSeriesSet replaceChildren(List<Expression> children) {
        return new TimeSeriesSet(source(), children.get(0), children.get(1), children.get(2));
    }

    @Override
    protected NodeInfo<TimeSeriesSet> info() {
        return NodeInfo.create(this, TimeSeriesSet::new, labelContainer(), label(), value());
    }
}
