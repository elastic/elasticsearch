/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IncreaseExponentialHistogramGroupingAggregatorFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TransportVersionAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Time series variant of {@link HistogramMerge}. This is the newer version of {@link DeltaOnlyHistogramMergeOverTime},
 * which support cumulative temporality for exponential histograms.
 */
public class HistogramMergeOverTime extends TimeSeriesAggregateFunction
    implements
        OptionalArgument,
        ToAggregator,
        TimestampAware,
        TemporalityAware,
        TransportVersionAware,
        SurrogateExpression {

    public static final TransportVersion INTRODUCTION_VERSION = TransportVersion.fromName("histogram_merge_over_time_cumulative_exp_histo");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "HistogramMergeOverTime_v2", // backwards compatibility
        HistogramMergeOverTime::readFrom
    );

    private final Expression timestamp;
    private final Expression temporality;

    @FunctionInfo(
        returnType = { "exponential_histogram", "tdigest" },
        type = FunctionType.TIME_SERIES_AGGREGATE,
        briefSummary = "Merges histogram field values over a time window."
    )
    public HistogramMergeOverTime(
        Source source,
        @Param(name = "histogram", type = { "exponential_histogram", "tdigest" }) Expression field,
        @Param(name = "window", type = "time_duration", optional = true) Expression window,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp, null);
    }

    public HistogramMergeOverTime(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        this(source, field, filter, window, timestamp, null);
    }

    public HistogramMergeOverTime(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression timestamp,
        @Nullable Expression temporality
    ) {
        super(source, field, filter, window, temporality == null ? List.of(timestamp) : List.of(timestamp, temporality));
        this.timestamp = timestamp;
        this.temporality = temporality;
    }

    private static HistogramMergeOverTime readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new HistogramMergeOverTime(
            source,
            field,
            filter,
            window,
            parameters.getFirst(),
            parameters.size() > 1 ? parameters.get(1) : null
        );
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt == DataType.EXPONENTIAL_HISTOGRAM || dt == DataType.TDIGEST,
            sourceText(),
            DEFAULT,
            "exponential_histogram",
            "tdigest"
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected NodeInfo<HistogramMergeOverTime> info() {
        if (temporality != null) {
            return NodeInfo.create(this, HistogramMergeOverTime::new, field(), filter(), window(), timestamp(), temporality);
        } else {
            return NodeInfo.create(
                this,
                (source, field, filter, window, timestamp) -> new HistogramMergeOverTime(source, field, filter, window, timestamp, null),
                field(),
                filter(),
                window(),
                timestamp()
            );
        }
    }

    @Override
    public HistogramMergeOverTime replaceChildren(List<Expression> newChildren) {
        return new HistogramMergeOverTime(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.size() > 4 ? newChildren.get(4) : null
        );
    }

    @Override
    public HistogramMergeOverTime withFilter(Expression filter) {
        return new HistogramMergeOverTime(source(), field(), filter, window(), timestamp, temporality);
    }

    @Override
    public boolean requiredTimeSeriesSource() {
        return true;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.EXPONENTIAL_HISTOGRAM) {
            return new IncreaseExponentialHistogramGroupingAggregatorFunction.FunctionSupplier(source());
        }
        if (type == DataType.TDIGEST) {
            throw new IllegalStateException("TDigest type should have caused a surrogate replacement");
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public HistogramMergeOverTime perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public Expression temporality() {
        return temporality;
    }

    @Override
    public HistogramMergeOverTime withTemporality(Expression newTemporality) {
        return new HistogramMergeOverTime(source(), field(), filter(), window(), timestamp(), newTemporality);
    }

    @Override
    public Expression forTransportVersion(TransportVersion minTransportVersion) {
        if (minTransportVersion.supports(INTRODUCTION_VERSION) == false) {
            DeltaOnlyHistogramMergeOverTime deltaUnsupported = new DeltaOnlyHistogramMergeOverTime(
                source(),
                field(),
                filter(),
                window(),
                temporality()
            );
            var replacement = deltaUnsupported.forTransportVersion(minTransportVersion);
            return replacement != null ? replacement : deltaUnsupported;
        }
        return null;
    }

    @Override
    public Expression surrogate() {
        if (field().dataType() == DataType.TDIGEST) {
            // TDigest doesn't support cumulative temporality
            return new DeltaOnlyHistogramMergeOverTime(source(), field(), filter(), window(), temporality());
        }
        return null;
    }
}
