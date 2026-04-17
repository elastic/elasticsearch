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
import java.util.Objects;

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

    private static final TransportVersion TIME_SERIES_AGGREGATE_EXEC_COLLAPSED = TransportVersion.fromName(
        "time_series_aggregate_collapsed"
    );

    private final Bucket timeBucket;
    private final Bucket outputTimeBucket;
    private final boolean collapsed;

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
        this(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize, timeBucket, timeBucket, false);
    }

    public TimeSeriesAggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        Integer estimatedRowSize,
        Bucket timeBucket,
        boolean collapsed
    ) {
        this(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize, timeBucket, timeBucket, collapsed);
    }

    public TimeSeriesAggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        Integer estimatedRowSize,
        Bucket timeBucket,
        Bucket outputTimeBucket,
        boolean collapsed
    ) {
        super(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
        this.timeBucket = timeBucket;
        this.outputTimeBucket = outputTimeBucket;
        this.collapsed = collapsed;
    }

    private TimeSeriesAggregateExec(StreamInput in) throws IOException {
        super(in);
        this.timeBucket = in.readOptionalWriteable(inp -> (Bucket) Bucket.ENTRY.reader.read(inp));
        if (in.getTransportVersion().supports(TimeSeriesAggregate.TIME_SERIES_OUTPUT_BUCKET)) {
            this.outputTimeBucket = in.readOptionalWriteable(inp -> (Bucket) Bucket.ENTRY.reader.read(inp));
        } else {
            this.outputTimeBucket = this.timeBucket;
        }
        if (in.getTransportVersion().supports(TIME_SERIES_AGGREGATE_EXEC_COLLAPSED)) {
            this.collapsed = in.readBoolean();
        } else {
            this.collapsed = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(timeBucket);
        if (out.getTransportVersion().supports(TimeSeriesAggregate.TIME_SERIES_OUTPUT_BUCKET)) {
            out.writeOptionalWriteable(outputTimeBucket);
        }
        if (out.getTransportVersion().supports(TIME_SERIES_AGGREGATE_EXEC_COLLAPSED)) {
            out.writeBoolean(collapsed);
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
            outputTimeBucket,
            collapsed
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
            outputTimeBucket,
            collapsed
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
            outputTimeBucket,
            collapsed
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
            outputTimeBucket,
            collapsed
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
            outputTimeBucket,
            collapsed
        );
    }

    public Bucket timeBucket() {
        return timeBucket;
    }

    public Bucket outputTimeBucket() {
        return outputTimeBucket;
    }

    public boolean isCollapsed() {
        return collapsed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timeBucket, outputTimeBucket, collapsed);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        TimeSeriesAggregateExec other = (TimeSeriesAggregateExec) obj;
        return Objects.equals(timeBucket, other.timeBucket)
            && Objects.equals(outputTimeBucket, other.outputTimeBucket)
            && collapsed == other.collapsed;
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

    public Rounding.Prepared outputTimeBucketRounding(FoldContext foldContext) {
        if (outputTimeBucket == null) {
            return null;
        }
        Rounding.Prepared rounding = outputTimeBucket.getDateRoundingOrNull(foldContext);
        if (rounding == null) {
            throw new EsqlIllegalArgumentException("expected output TBUCKET; got ", outputTimeBucket);
        }
        return rounding;
    }
}
