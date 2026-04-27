/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
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

            The `step` can be a time duration (e.g. `1 hour`) or a target bucket count (an integer).
            When a count is given, the actual step is chosen as the finest supported granularity
            (from 1 millisecond up to 1 day) that produces at most `count` buckets in the `[from, to]` range.
            Explicit `from` and `to` bounds are required when a count is given.

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
            @Example(description = "The same query with explicit bounds:", file = "tstep", tag = "docsTStepExplicitBounds"),
            @Example(
                description = "The same query using a target bucket count of 2 instead of a fixed step:",
                file = "tstep",
                tag = "docsTStepBucketCount"
            ) },
        type = FunctionType.GROUPING
    )
    public TStep(
        Source source,
        @Param(name = "step", type = { "time_duration", "integer", "long" }, description = """
            Fixed bucket width in UTC, or target number of buckets to fill the `from`-`to` range.
            When an integer count is given, the actual step is chosen as the finest supported granularity
            (from 1 millisecond up to 1 day) that produces at most `count` buckets in the `[from, to]` range.
            Explicit `from` and `to` bounds are required when a count is given.""") Expression step,
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
        // ResolveTimestampBoundsAware (Batch 1 "Initialize") runs before ImplicitCasting
        // (Batch 2 "Resolution"), so a string literal like "1 hour" still has KEYWORD type here.
        // Accept foldable KEYWORD as a proxy for "will become TIME_DURATION after casting", so
        // bounds are injected before postAnalysisVerification runs.
        // Integer (bucket-count) steps intentionally stay excluded: they require explicit from/to
        // bounds and must not silently inherit them from the request filter.
        return step.resolved()
            && (step.dataType() == DataType.TIME_DURATION || (step.dataType() == DataType.KEYWORD && step.foldable()))
            && (start == null && end == null);
    }

    @Override
    public Expression withTimestampBounds(Literal startBound, Literal endBound) {
        Expression newStart = start != null ? start : startBound;
        Expression newEnd = end != null ? end : endBound;
        return new TStep(source(), step, newStart, newEnd, timestamp, configuration);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (step.resolved() && step.dataType() == DataType.TIME_DURATION && start == null && end == null) {
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

        Expression effectiveStep = step.dataType().isWholeNumber()
            ? Literal.timeDuration(source(), deriveStepDuration(FoldContext.small()))
            : step;

        var bucket = new Bucket(
            source(),
            newTimestamp,
            effectiveStep,
            null,
            null,
            configuration.withZoneId(ZoneOffset.UTC),
            offset(FoldContext.small())
        );

        return new Add(source(), bucket, effectiveStep, configuration);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var resolution = isType(
            step,
            dt -> dt == DataType.TIME_DURATION || dt.isWholeNumber(),
            sourceText(),
            FIRST,
            "time_duration",
            "integer",
            "long"
        ).and(isType(timestamp, DataType::isMillisOrNanos, sourceText(), IMPLICIT, "date_nanos or datetime"));
        if (resolution.unresolved()) {
            return resolution;
        }
        if (step.dataType().isWholeNumber()) {
            if (start == null || end == null) {
                return new TypeResolution(
                    org.elasticsearch.common.Strings.format(
                        "[%s] requires 'from' and 'to' bounds when step is a bucket count",
                        sourceText()
                    )
                );
            }
            if (step.foldable()) {
                long count = ((Number) step.fold(FoldContext.small())).longValue();
                if (count <= 0 || count > Integer.MAX_VALUE) {
                    return new TypeResolution(
                        org.elasticsearch.common.Strings.format(
                            "[%s] requires a bucket count between 1 and %d, got [%d]",
                            sourceText(),
                            Integer.MAX_VALUE,
                            count
                        )
                    );
                }
            }
            resolution = resolution.and(isStringOrDateBound(start, SECOND));
            if (resolution.unresolved()) {
                return resolution;
            }
            return resolution.and(isStringOrDateBound(end, THIRD));
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
        Expression effectiveStep = step.dataType().isWholeNumber()
            ? Literal.timeDuration(source(), deriveStepDuration(FoldContext.small()))
            : step;
        return new Bucket(
            source(),
            timestamp,
            effectiveStep,
            null,
            null,
            configuration.withZoneId(ZoneOffset.UTC),
            offset(FoldContext.small())
        );
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    private long offset(FoldContext foldContext) {
        long stepMs = step.dataType().isWholeNumber()
            ? deriveStepDuration(foldContext).toMillis()
            : ((Duration) step.fold(foldContext)).toMillis();
        if (stepMs == 0) {
            // {@link Bucket} doesn't support nanosecond step precision
            return 0;
        }

        if ((start != null && start.foldable()) == false) {
            throw new EsqlIllegalArgumentException(NAME + " requires a start bound");
        }

        return DateUtils.floorRemainder(foldBoundToMillis(start, foldContext), stepMs);
    }

    /**
     * Derives a fixed-width step duration from the target bucket count and the [from, to] range
     * by delegating to {@link Bucket.DateRoundingPicker}.
     */
    private Duration deriveStepDuration(FoldContext foldContext) {
        int count = ((Number) step.fold(foldContext)).intValue();
        long fromMs = foldBoundToMillis(start, foldContext);
        long toMs = foldBoundToMillis(end, foldContext);
        // Use primary-only picking to preserve TSTEP's fixed-width (1ms–1day) contract.
        // When no primary unit fits the requested count the range exceeds 1-day * count,
        // so 1 day is the finest granularity that makes sense.
        Rounding picked = new Bucket.DateRoundingPicker(count, fromMs, toMs, ZoneOffset.UTC, 0L).pickPrimaryRounding();
        if (picked == null) {
            return Duration.ofDays(1);
        }
        var rounding = picked.prepareForUnknown();
        long fromRounded = rounding.round(fromMs);
        return Duration.ofMillis(rounding.nextRoundingValue(fromRounded) - fromRounded);
    }

    private long foldBoundToMillis(Expression bound, FoldContext foldContext) {
        var folded = bound.fold(foldContext);
        if (DataType.isString(bound.dataType())) {
            return dateTimeToLong(((BytesRef) folded).utf8ToString());
        }
        long value = ((Number) folded).longValue();
        return bound.dataType() == DataType.DATE_NANOS ? DateUtils.toMilliSeconds(value) : value;
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
