/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.common.Failures;
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
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.IMPLICIT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Creates fixed-width, end-aligned time buckets anchored to the end of {@code TRANGE} or {@code now()}.
 * <p>
 * Unlike {@link TBucket}, {@code TSTEP} uses a fixed UTC alignment to stay independent of the configured timezone.
 */
public class TStep extends GroupingFunction.EvaluatableGroupingFunction
    implements
        OnlySurrogateExpression,
        TimestampAware,
        TimestampBoundsAware.OfExpression,
        ConfigurationFunction {
    public static final String NAME = "TStep";

    private final Configuration configuration;
    private final Expression step;
    private final Expression end;
    private final Expression timestamp;

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        description = """
            Creates end-aligned time buckets over @timestamp using a fixed step. The end anchor is taken from TRANGE when present,
            or from the current query time otherwise.""",
        examples = {
            @Example(
                description = "Provide a step size and align buckets to the end of TRANGE.",
                file = "tstep",
                tag = "docsTStepByOneHourDuration"
            ),
            @Example(
                description = "Provide a string representation of the step size and align buckets to the end of TRANGE.",
                file = "tstep",
                tag = "docsTStepByOneHourDurationAsString"
            ) },
        type = FunctionType.GROUPING
    )
    public TStep(
        Source source,
        @Param(
            name = "step",
            type = { "time_duration" },
            description = "Fixed step size. Buckets are anchored to the end of TRANGE, or the current query time when TRANGE is absent."
        ) Expression step,
        Expression timestamp,
        Configuration configuration
    ) {
        this(source, step, null, timestamp, configuration);
    }

    private TStep(Source source, Expression step, Expression end, Expression timestamp, Configuration configuration) {
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
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("should be rewritten");
    }

    @Override
    public Expression surrogate() {
        return new Add(source(), shiftedBucket(), step, configuration);
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

    @Override
    public boolean needsTimestampBounds() {
        return end == null;
    }

    @Override
    public Expression withTimestampBounds(Literal start, Literal end) {
        return withEnd(normalizeTimestampBound(end));
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        // TSTEP can always fall back to TRANGE or query-time now().
    }

    /**
     * Returns the aligned bucket lattice used by {@code TSTEP}. The reported {@code TSTEP} values are the lattice points
     * themselves, while bucket membership is computed as {@code (previous_lattice_point, lattice_point]}.
     */
    public Bucket timeBucket() {
        return new Bucket(source(), timestamp, step, null, null, ConfigurationAware.CONFIGURATION_MARKER, offset(FoldContext.small()));
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    private Bucket shiftedBucket() {
        return new Bucket(
            source(),
            new Sub(source(), timestamp, oneTick(), configuration),
            step,
            null,
            null,
            ConfigurationAware.CONFIGURATION_MARKER,
            offset(FoldContext.small())
        );
    }

    private Expression oneTick() {
        Duration tick = timestamp.dataType() == DataType.DATE_NANOS ? Duration.ofNanos(1) : Duration.ofMillis(1);
        return Literal.timeDuration(source(), tick);
    }

    private Expression resolvedEnd() {
        if (end != null) {
            return end;
        }
        Instant now = configuration.now();
        long value = timestamp.dataType() == DataType.DATE_NANOS ? DateUtils.toLong(now) : now.toEpochMilli();
        return new Literal(source(), value, timestamp.dataType());
    }

    private Literal normalizeTimestampBound(Literal bound) {
        if (bound.dataType() == timestamp.dataType()) {
            return bound;
        }
        long millis = bound.dataType() == DataType.DATE_NANOS
            ? DateUtils.toMilliSeconds(((Number) bound.value()).longValue())
            : ((Number) bound.value()).longValue();
        long value = timestamp.dataType() == DataType.DATE_NANOS ? DateUtils.toLong(Instant.ofEpochMilli(millis)) : millis;
        return new Literal(bound.source(), value, timestamp.dataType());
    }

    private long offset(FoldContext foldContext) {
        long stepMillis = ((Duration) step.fold(foldContext)).toMillis();
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
