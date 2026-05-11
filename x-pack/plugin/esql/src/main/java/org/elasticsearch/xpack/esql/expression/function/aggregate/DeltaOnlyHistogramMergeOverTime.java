/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DeltaOnlyHistogramMergeOverTimeExponentialHistogramAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DeltaOnlyHistogramMergeOverTimeTDigestAggregatorFunctionSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Time series variant of {@link HistogramMerge}. In addition to merging histograms,
 * this implementation also loads the temporality and ignores cumulative histograms (with a warning).
 * <p>
 * For backwards compatibility, the intermediate state produced by this aggregation is compatible with {@link HistogramMerge}:
 * In previous versions, we used {@link HistogramMerge} directly as per-timeseries aggregation.
 */
public class DeltaOnlyHistogramMergeOverTime extends TimeSeriesAggregateFunction
    implements
        OptionalArgument,
        ToAggregator,
        TemporalityAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "HistogramMergeOverTime", // backwards compatibility
        DeltaOnlyHistogramMergeOverTime::readFrom
    );

    private final Expression temporality;

    @FunctionInfo(returnType = { "exponential_histogram", "tdigest" }, type = FunctionType.TIME_SERIES_AGGREGATE)
    public DeltaOnlyHistogramMergeOverTime(
        Source source,
        @Param(name = "histogram", type = { "exponential_histogram", "tdigest" }) Expression field,
        @Param(name = "window", type = "time_duration", optional = true) Expression window
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), null);
    }

    public DeltaOnlyHistogramMergeOverTime(Source source, Expression field, Expression filter, Expression window) {
        this(source, field, filter, window, null);
    }

    public DeltaOnlyHistogramMergeOverTime(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        @Nullable Expression temporality
    ) {
        super(source, field, filter, window, temporality == null ? emptyList() : List.of(temporality));
        this.temporality = temporality;
    }

    private static DeltaOnlyHistogramMergeOverTime readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new DeltaOnlyHistogramMergeOverTime(source, field, filter, window, parameters.isEmpty() ? null : parameters.getFirst());
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
    protected NodeInfo<DeltaOnlyHistogramMergeOverTime> info() {
        if (temporality != null) {
            return NodeInfo.create(this, DeltaOnlyHistogramMergeOverTime::new, field(), filter(), window(), temporality);
        } else {
            return NodeInfo.create(
                this,
                (source, field, filter, window) -> new DeltaOnlyHistogramMergeOverTime(source, field, filter, window, null),
                field(),
                filter(),
                window()
            );
        }
    }

    @Override
    public DeltaOnlyHistogramMergeOverTime replaceChildren(List<Expression> newChildren) {
        return new DeltaOnlyHistogramMergeOverTime(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.size() > 3 ? newChildren.get(3) : null
        );
    }

    @Override
    public DeltaOnlyHistogramMergeOverTime withFilter(Expression filter) {
        return new DeltaOnlyHistogramMergeOverTime(source(), field(), filter, window(), temporality);
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.EXPONENTIAL_HISTOGRAM) {
            return new DeltaOnlyHistogramMergeOverTimeExponentialHistogramAggregatorFunctionSupplier(source());
        }
        if (type == DataType.TDIGEST) {
            return new DeltaOnlyHistogramMergeOverTimeTDigestAggregatorFunctionSupplier(source());
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public DeltaOnlyHistogramMergeOverTime perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public Expression temporality() {
        return temporality;
    }

    @Override
    public DeltaOnlyHistogramMergeOverTime withTemporality(Expression newTemporality) {
        return new DeltaOnlyHistogramMergeOverTime(source(), field(), filter(), window(), newTemporality);
    }

}
