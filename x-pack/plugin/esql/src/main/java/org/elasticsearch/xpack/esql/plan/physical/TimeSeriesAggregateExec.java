/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;

import java.io.IOException;
import java.util.List;

/**
 * An extension of {@link Aggregate} to perform time-series aggregation per time-series, such as rate or _over_time.
 * The grouping must be `_tsid` and `tbucket` or just `_tsid`.
 */
public class TimeSeriesAggregateExec extends AggregateExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TimeSeriesAggregateExec",
        TimeSeriesAggregateExec::new
    );

    private final Bucket timeBucket;

    public TimeSeriesAggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        Integer estimatedRowSize,
        Bucket timeBucket
    ) {
        super(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
        this.timeBucket = timeBucket;
    }

    private TimeSeriesAggregateExec(StreamInput in) throws IOException {
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
    protected NodeInfo<AggregateExec> info() {
        return NodeInfo.create(
            this,
            TimeSeriesAggregateExec::new,
            child(),
            groupings(),
            aggregates(),
            getMode(),
            intermediateAttributes(),
            estimatedRowSize(),
            timeBucket
        );
    }

    @Override
    public TimeSeriesAggregateExec replaceChild(PhysicalPlan newChild) {
        return new TimeSeriesAggregateExec(
            source(),
            newChild,
            groupings(),
            aggregates(),
            getMode(),
            intermediateAttributes(),
            estimatedRowSize(),
            timeBucket
        );
    }

    @Override
    public TimeSeriesAggregateExec withAggregates(List<? extends NamedExpression> aggregates) {
        return new TimeSeriesAggregateExec(
            source(),
            child(),
            groupings,
            aggregates,
            mode,
            intermediateAttributes,
            estimatedRowSize,
            timeBucket
        );
    }

    @Override
    public TimeSeriesAggregateExec withMode(AggregatorMode newMode) {
        return new TimeSeriesAggregateExec(
            source(),
            child(),
            groupings(),
            aggregates(),
            newMode,
            intermediateAttributes(),
            estimatedRowSize(),
            timeBucket
        );
    }

    @Override
    protected TimeSeriesAggregateExec withEstimatedSize(int estimatedRowSize) {
        return new TimeSeriesAggregateExec(
            source(),
            child(),
            groupings(),
            aggregates(),
            getMode(),
            intermediateAttributes(),
            estimatedRowSize,
            timeBucket
        );
    }

    public Bucket timeBucket() {
        return timeBucket;
    }

    public Rounding.Prepared timeBucketRounding(FoldContext foldContext) {
        if (timeBucket == null) {
            return null;
        }
        Rounding.Prepared rounding = timeBucket.getDateRoundingOrNull(foldContext);
        if (rounding == null) {
            throw new EsqlIllegalArgumentException("expected TBUCKET; got ", timeBucket);
        }
        return rounding;
    }
}
