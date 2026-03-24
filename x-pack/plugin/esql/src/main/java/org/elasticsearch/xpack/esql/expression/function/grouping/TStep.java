/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.IMPLICIT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Creates fixed-width, end-aligned time buckets over the {@code @timestamp}.
 * <p>
 * Unlike {@link TBucket}, which aligns buckets at their <em>start</em> and respects the configured
 * timezone, {@code TSTEP} buckets aligned at the <em>end</em> and always anchors to UTC. This makes it
 * suitable for PromQL range queries where the most-recent bucket should end exactly at the
 * query boundary.
 */
public class TStep extends GroupingFunction.EvaluatableGroupingFunction
    implements
        OnlySurrogateExpression,
        TimestampAware,
        OptionalArgument,
        ConfigurationFunction {
    public static final String NAME = "TStep";

    private final Configuration configuration;
    private final Expression step;
    private final Expression end;
    private final Expression timestamp;

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        description = """
            Creates groups of values - buckets - out of a `@timestamp` attribute.
            Unlike `TBUCKET`, which aligns buckets at their `start`, `TSTEP` aligns at the `end` and anchors
            to UTC regardless of the configured timezone.""",
        examples = {
            @Example(
                description = "Provide a step size to create end-aligned buckets anchored to the end of `TRANGE`:",
                file = "tstep",
                tag = "docsTStepByOneHourDuration",
                explanation = """
                    The returned `bucket` values are the `end` timestamps of each bucket.
                    For example, the bucket labelled `2023-10-23T12:55:01.543Z` contains all data points with
                    `@timestamp` in `(2023-10-23T11:55:01.543Z, 2023-10-23T12:55:01.543Z]`. The bucket
                    boundaries are stepped back from the end of `TRANGE` (`13:55:01.543Z`) in exact 1-hour
                    increments, so every boundary aligns to that anchor."""
            ),
            @Example(
                description = "The step size can also be provided as a string:",
                file = "tstep",
                tag = "docsTStepByOneHourDurationAsString"
            ) },
        type = FunctionType.GROUPING
    )
    public TStep(Source source, @Param(name = "step", type = { "time_duration" }, description = """
        Fixed bucket width. One bucket boundary falls exactly on the end of `TRANGE`,
        or on the current query time when `TRANGE` is absent; all other boundaries are integer
        multiples of `step` away from that anchor.""") Expression step, Expression timestamp, Configuration configuration) {
        this(source, step, null, timestamp, configuration);
    }

    public TStep(Source source, Expression step, Expression end, Expression timestamp, Configuration configuration) {
        super(source, end == null ? List.of(step, timestamp) : List.of(step, end, timestamp));
        this.step = step;
        this.end = end;
        this.timestamp = timestamp;
        this.configuration = configuration;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("should be rewritten");
    }

    @Override
    public Expression surrogate() {
        Expression tick = Literal.timeDuration(
            source(),
            timestamp.dataType() == DataType.DATE_NANOS ? Duration.ofNanos(1) : Duration.ofMillis(1)
        );
        // `Bucket` is start-aligned and right-open. `TSTEP` wants end-aligned and right-closed.
        // Subtracting one tick fixes exact-boundary timestamps, which would
        // otherwise get pushed one bucket too far.
        return new Add(
            source(),
            new Bucket(
                source(),
                new Sub(source(), timestamp, tick, configuration),
                step,
                null,
                null,
                configuration.withZoneId(ZoneOffset.UTC),
                offset(FoldContext.small())
            ),
            step,
            configuration
        );
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isType(step, dt -> dt == DataType.TIME_DURATION, sourceText(), FIRST, "time_duration").and(
            isType(timestamp, DataType::isMillisOrNanos, sourceText(), IMPLICIT, "date_nanos or datetime")
        );
        if (resolution.unresolved() || end == null) {
            return resolution;
        }
        return resolution.and(isType(end, DataType::isMillisOrNanos, sourceText(), SECOND, "date_nanos or datetime"));
    }

    @Override
    public DataType dataType() {
        if (timestamp.resolved() == false) {
            return DataType.UNSUPPORTED;
        }
        return timestamp.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TStep(
            source(),
            newChildren.getFirst(),
            newChildren.size() == 3 ? newChildren.get(1) : null,
            newChildren.getLast(),
            configuration
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TStep::new, step, end, timestamp, configuration);
    }

    public Configuration configuration() {
        return configuration;
    }

    public Expression step() {
        return step;
    }

    public Expression end() {
        return end;
    }

    public TStep withEnd(Expression newEnd) {
        if (Objects.equals(end, newEnd)) {
            return this;
        }
        return new TStep(source(), step, newEnd, timestamp, configuration);
    }

    public Bucket timeBucketSpecRef() {
        return new Bucket(source(), timestamp, step, null, null, configuration.withZoneId(ZoneOffset.UTC), offset(FoldContext.small()));
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    private Expression resolvedEnd() {
        if (end != null) {
            return end;
        }
        Instant now = configuration.now();
        long value = timestamp.dataType() == DataType.DATE_NANOS ? DateUtils.toLong(now) : now.toEpochMilli();
        return new Literal(source(), value, timestamp.dataType());
    }

    private long offset(FoldContext foldContext) {
        long stepMillis = ((Duration) step.fold(foldContext)).toMillis();
        if (stepMillis == 0) {
            // sub-millisecond step: rounding infrastructure works in ms, so offset is 0
            return 0;
        }
        long endMillis = ((Number) resolvedEnd().fold(foldContext)).longValue();
        if (resolvedEnd().dataType() == DataType.DATE_NANOS) {
            endMillis = DateUtils.toMilliSeconds(endMillis);
        }
        return DateUtils.floorRemainder(endMillis, stepMillis);
    }

    @Override
    public String toString() {
        return "TStep{step=" + step + ", end=" + (end == null ? "<trange-or-now>" : end) + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), configuration);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        TStep other = (TStep) obj;
        return configuration.equals(other.configuration);
    }
}
