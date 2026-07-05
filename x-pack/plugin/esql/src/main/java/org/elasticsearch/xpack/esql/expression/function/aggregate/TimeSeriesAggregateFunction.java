/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Extends {@link AggregateFunction} to support aggregation per time_series,
 * such as {@link Rate} or {@link MaxOverTime}.
 */
public abstract class TimeSeriesAggregateFunction extends AggregateFunction implements OptionalArgument {

    protected TimeSeriesAggregateFunction(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        List<? extends Expression> parameters
    ) {
        super(source, field, filter, window, parameters);
    }

    protected TimeSeriesAggregateFunction(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Returns the aggregation function to be used in the first aggregation stage,
     * which is grouped by `_tsid` (and `time_bucket`).
     *
     * @see org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate
     */
    public abstract AggregateFunction perTimeSeriesAggregation();

    public boolean requiredTimeSeriesSource() {
        return false;
    }

    @Override
    public List<Attribute> aggregateInputReferences(Supplier<List<Attribute>> inputAttributes) {
        if (requiredTimeSeriesSource()) {
            List<? extends Expression> parameters = parameters();
            List<Attribute> attributes = new ArrayList<>();
            attributes.addAll(field().references());
            for (Expression p : parameters) {
                attributes.addAll(p.references());
            }
            for (Attribute attr : inputAttributes.get()) {
                for (EsField f : EsQueryExec.TIME_SERIES_SOURCE_FIELDS) {
                    if (attr.name().equals(f.getName())) {
                        attributes.addAll(attr.references());
                        break;
                    }
                }
            }
            return attributes;
        } else {
            return super.aggregateInputReferences(inputAttributes);
        }
    }
}
