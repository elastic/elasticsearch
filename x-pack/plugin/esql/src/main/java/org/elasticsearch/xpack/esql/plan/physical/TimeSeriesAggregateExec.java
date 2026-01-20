/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

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

    private static final TransportVersion TIME_SERIES_AGGREGATE_EXEC_TIMESTAMP_VALUE_RANGE = TransportVersion.fromName(
        "time_series_aggregate_exec_timestamp_value_range"
    );

    private final Bucket timeBucket;

    @Nullable
    private final TimeSeriesAggregate.TimeRange timeRange;

    public TimeSeriesAggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        Integer estimatedRowSize,
        Bucket timeBucket,
        TimeSeriesAggregate.TimeRange timeRange
    ) {
        super(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
        this.timeBucket = timeBucket;
        this.timeRange = timeRange;
    }

    private TimeSeriesAggregateExec(StreamInput in) throws IOException {
        super(in);
        this.timeBucket = in.readOptionalWriteable(inp -> (Bucket) Bucket.ENTRY.reader.read(inp));
        if (in.getTransportVersion().supports(TIME_SERIES_AGGREGATE_EXEC_TIMESTAMP_VALUE_RANGE)) {
            if (in.readBoolean()) {
                this.timeRange = new TimeSeriesAggregate.TimeRange(in.readLong(), in.readLong());
            } else {
                this.timeRange = null;
            }
        } else {
            this.timeRange = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(timeBucket);
        if (out.getTransportVersion().supports(TIME_SERIES_AGGREGATE_EXEC_TIMESTAMP_VALUE_RANGE)) {
            if (timeRange != null) {
                out.writeBoolean(true);
                out.writeLong(timeRange.min());
                out.writeLong(timeRange.max());
            } else {
                out.writeBoolean(false);
            }
        }
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
            timeBucket,
            timeRange
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
            timeBucket,
            timeRange
        );
    }

    @Override
    public AggregateExec withAggregates(List<? extends NamedExpression> newAggregates) {
        return new TimeSeriesAggregateExec(
            source(),
            child(),
            groupings(),
            newAggregates,
            getMode(),
            intermediateAttributes(),
            estimatedRowSize(),
            timeBucket,
            timeRange
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
            timeBucket,
            timeRange
        );
    }

    @Override
    protected AggregateExec withEstimatedSize(int estimatedRowSize) {
        return new TimeSeriesAggregateExec(
            source(),
            child(),
            groupings(),
            aggregates(),
            getMode(),
            intermediateAttributes(),
            estimatedRowSize,
            timeBucket,
            timeRange
        );
    }

    public Bucket timeBucket() {
        return timeBucket;
    }

    public TimeSeriesAggregate.TimeRange timeRange() {
        return timeRange;
    }

    public Rounding.Prepared timeBucketRounding(FoldContext foldContext) {
        if (timeBucket == null) {
            return null;
        }
        Long minTimestamp = timeRange == null ? null : timeRange.min();
        Long maxTimestamp = timeRange == null ? null : timeRange.max();
        Rounding.Prepared rounding = timeBucket.getDateRoundingOrNull(foldContext, minTimestamp, maxTimestamp);
        if (rounding == null) {
            throw new EsqlIllegalArgumentException("expected TBUCKET; got ", timeBucket);
        }
        return rounding;
    }
}
