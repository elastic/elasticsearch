/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.IMPLICIT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * Groups {@code @timestamp} into fixed-width time buckets for aggregation, labeling each bucket by its end instant.
 * <p>
 * The grid is anchored using the <strong>start</strong> of the {@code @timestamp} range from the request query filter
 * (see {@link TimestampBoundsAware}). {@code TSTEP} is not compatible with {@code TRANGE} in the same query at the moment.
 * <p>
 * {@link TBucket} aligns buckets to calendar boundaries or an epoch-based grid in the configured time zone.
 * {@code TSTEP} uses a contiguous {@code step}-wide grid in UTC and returns end-labeled bucket timestamps.
 */
public class TStep extends GroupingFunction.EvaluatableGroupingFunction
    implements
        OnlySurrogateExpression,
        TimestampAware,
        TimestampBoundsAware.OfExpression,
        TwoOptionalArguments,
        ConfigurationFunction {
    public static final String NAME = "TStep";

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TStep.class).quaternaryConfig(TStep::new).name("tstep");

    public static final Duration ONE_NANOSECOND = Duration.ofNanos(1);
    public static final Duration ONE_MILLISECOND = Duration.ofMillis(1);

    private final Configuration configuration;
    private final Expression step;
    @Nullable
    private final Expression start;
    @Nullable
    private final Expression end;
    private final Expression timestamp;

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        description = """
            Creates groups of values - buckets - out of a `@timestamp` attribute using a fixed `step` width.

            Unlike [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md),
            which aligns buckets to calendar boundaries, `TSTEP` always buckets at the fixed interval increments in UTC timezone.
            Each `bucket` label is rendered as the upper boundary of the half-open interval `(timestamp - step, timestamp]`.

            In the one-argument form, provide a
            [`@timestamp` range](docs-content://explore-analyze/query-filter/languages/esql-kibana.md#_standard_time_filter)
            in the request query filter; that range's start anchors the grid.
            In the three-argument form, supply explicit `from` and `to` bounds directly; these take precedence over any
            request `@timestamp` filter. `TSTEP` cannot be used together with `TRANGE`.""",
        examples = {
            @Example(
                description = """
                    Group `@timestamp` into one-hour buckets when the request includes a `@timestamp` filter:""",
                file = "tstep",
                tag = "docsTStepByOneHourDuration",
                explanation = """
                    The returned `bucket` values are the end timestamps of each bucket.
                    Boundaries are generated as `range_start + n * step` (UTC), and each bucket
                    represents `(bucket_end - step, bucket_end]`."""
            ),
            @Example(description = "The same query with explicit bounds:", file = "tstep", tag = "docsTStepExplicitBounds") },
        type = FunctionType.GROUPING
    )
    public TStep(
        Source source,
        @Param(name = "step", type = { "time_duration" }, description = """
            Fixed bucket width in UTC. Bucket boundaries are spaced by `step` from the start of the time range.""") Expression step,
        @Param(
            name = "from",
            type = { "date", "date_nanos", "keyword" },
            description = """
                Start of the time range that anchors the step grid. Required together with `to`.""",
            optional = true
        ) @Nullable Expression start,
        @Param(name = "to", type = { "date", "date_nanos", "keyword" }, description = """
            End of the time range. Required together with `from`.""", optional = true) @Nullable Expression end,
        Expression timestamp,
        Configuration configuration
    ) {
        super(source, Bucket.fields(step, timestamp, start, end));
        this.step = step;
        this.start = start;
        this.end = end;
        this.timestamp = timestamp;
        this.configuration = configuration;
    }

    public TStep(Source source, Expression step, Expression timestamp, Configuration configuration) {
        this(source, step, null, null, timestamp, configuration);
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
    public boolean needsTimestampBounds() {
        return step.resolved() && (start == null && end == null);
    }

    @Override
    public Expression withTimestampBounds(Literal startBound, Literal endBound) {
        Expression newStart = start != null ? start : startBound;
        Expression newEnd = end != null ? end : endBound;
        return new TStep(source(), step, newStart, newEnd, timestamp, configuration);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (step.resolved() && start == null && end == null) {
            failures.add(
                Failure.fail(
                    this,
                    "[{}] requires either a `@timestamp` range in the request query filter"
                        + " or explicit `from` and `to` parameters to anchor the step grid",
                    sourceText()
                )
            );
        }
    }

    /**
    * Replace {@link TStep} expression with {@link Bucket} expression.
    * <p>
    * Bucket uses truncation-style, left-labeled intervals on calendar-aligned grid:
    * Bucket(t) = left, for t in [label:=left, right)
    * e.g., step=1h: [12:00; 13:00) [13:00; 14:00)
    * <p>
    * TStep uses right-labeled intervals on a start-aligned grid:
    * TStep(t) = right, for t in (left, label:=right]
    * e.g., step=1h, start=12:13: (12:13; 13:13] (13:13; 14:13]
    * <p>
    * Therefore, TStep(t) := Bucket0(t - tick) + step, where Bucket0 is Bucket with offset = start mod step.
    */

    @Override
    public Expression surrogate() {
        var newTimestamp = new Sub(
            source(),
            timestamp,
            Literal.timeDuration(source(), timestamp.dataType() == DataType.DATE_NANOS ? ONE_NANOSECOND : ONE_MILLISECOND),
            configuration
        );

        var bucket = new Bucket(
            source(),
            newTimestamp,
            step,
            null,
            null,
            configuration.withZoneId(ZoneOffset.UTC),
            offset(FoldContext.small())
        );

        return new Add(source(), bucket, step, configuration);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var resolution = isType(step, dt -> dt == DataType.TIME_DURATION, sourceText(), FIRST, "time_duration").and(
            isType(timestamp, DataType::isMillisOrNanos, sourceText(), IMPLICIT, "date_nanos or datetime")
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        if ((start == null) != (end == null)) {
            return new TypeResolution(
                org.elasticsearch.common.Strings.format("[%s] requires both 'from' and 'to' arguments, or neither", sourceText())
            );
        }
        if (start != null) {
            resolution = resolution.and(isStringOrDateBound(start, SECOND));
            if (resolution.unresolved()) {
                return resolution;
            }
            return resolution.and(isStringOrDateBound(end, THIRD));
        }
        return resolution;
    }

    private TypeResolution isStringOrDateBound(Expression bound, TypeResolutions.ParamOrdinal ordinal) {
        return isType(
            bound,
            dt -> DataType.isString(dt) || DataType.isMillisOrNanos(dt),
            sourceText(),
            ordinal,
            "date_nanos or datetime",
            "string"
        );
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
        Expression start = newChildren.size() > 2 ? newChildren.get(2) : null;
        Expression end = newChildren.size() > 3 ? newChildren.get(3) : null;
        return new TStep(source(), newChildren.get(0), start, end, newChildren.get(1), configuration);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TStep::new, step, start, end, timestamp, configuration);
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

    public Expression start() {
        return start;
    }

    public Bucket timeBucketSpecRef() {
        return new Bucket(source(), timestamp, step, null, null, configuration.withZoneId(ZoneOffset.UTC), offset(FoldContext.small()));
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    private long offset(FoldContext foldContext) {
        long stepMs = ((Duration) step.fold(foldContext)).toMillis();
        if (stepMs == 0) {
            // {@link Bucket} doesn't support nanosecond step precision
            return 0;
        }

        if ((start != null && start.foldable()) == false) {
            throw new EsqlIllegalArgumentException(NAME + " requires a start bound");
        }

        var folded = start.fold(foldContext);
        long startMs;
        if (DataType.isString(start.dataType())) {
            startMs = dateTimeToLong(((BytesRef) folded).utf8ToString());
        } else {
            startMs = ((Number) folded).longValue();
            if (start.dataType() == DataType.DATE_NANOS) {
                startMs = DateUtils.toMilliSeconds(startMs);
            }
        }
        return DateUtils.floorRemainder(startMs, stepMs);
    }

    @Override
    public String toString() {
        return NAME + "{step=" + step + ", start=" + start + ", end=" + end + "}";
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
