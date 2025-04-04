/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;

import java.io.IOException;
import java.util.List;

/**
 * An extension of {@link Aggregate} to perform time-series aggregation per time-series, such as rate or _over_time.
 * The grouping must be `_tsid` and `tbucket` or just `_tsid`.
 */
public class TimeSeriesAggregate extends Aggregate {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TimeSeriesAggregate",
        TimeSeriesAggregate::new
    );

    private final Bucket timeBucket;

    public TimeSeriesAggregate(
        Source source,
        LogicalPlan child,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates,
        Bucket timeBucket
    ) {
        super(source, child, groupings, aggregates);
        this.timeBucket = timeBucket;
    }

    public TimeSeriesAggregate(StreamInput in) throws IOException {
        super(in);
        this.timeBucket = in.readOptionalWriteable(inp -> (Bucket) Bucket.ENTRY.reader.read(inp));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(timeBucket);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, TimeSeriesAggregate::new, child(), groupings, aggregates, timeBucket);
    }

    @Override
    public TimeSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new TimeSeriesAggregate(source(), newChild, groupings, aggregates, timeBucket);
    }

    @Override
    public TimeSeriesAggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new TimeSeriesAggregate(source(), child, newGroupings, newAggregates, timeBucket);
    }

    @Nullable
    public Bucket timeBucket() {
        return timeBucket;
    }
}
