/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * Extends {@link AggregateFunction} to support aggregation per time_series,
 * such as {@link Rate} or {@link MaxOverTime}.
 */
public abstract class TimeSeriesAggregateFunction extends AggregateFunction implements OptionalArgument {

    protected TimeSeriesAggregateFunction(
        Source source,
        List<? extends Expression> fields,
        Expression filter,
        Expression window,
        List<? extends Expression> parameters
    ) {
        super(source, fields, filter, window, parameters);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Legacy serialization format for backwards compatibility
        List<? extends Expression> fields = fields();
        source().writeTo(out);
        out.writeNamedWriteable(fields.get(0));
        out.writeNamedWriteable(filter());
        if (out.getTransportVersion().supports(WINDOW_INTERVAL)) {
            out.writeNamedWriteable(window());
        }
        out.writeNamedWriteableCollection(CollectionUtils.combine(fields.subList(1, fields.size()), parameters()));
    }

    public final Expression field() {
        return fields().get(0);
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isExact(field(), sourceText(), DEFAULT);
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
        List<Attribute> attributes = new ArrayList<>(super.aggregateInputReferences(inputAttributes));
        if (requiredTimeSeriesSource()) {
            for (Attribute attr : inputAttributes.get()) {
                for (EsField f : EsQueryExec.TIME_SERIES_SOURCE_FIELDS) {
                    if (attr.name().equals(f.getName())) {
                        attributes.addAll(attr.references());
                        break;
                    }
                }
            }
        }
        return attributes;
    }
}
