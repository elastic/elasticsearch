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
import java.util.Locale;
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

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TStep.class)
        .quaternaryConfig(TStep::new)
        .name(NAME.toLowerCase(Locale.ROOT));

    private final Configuration configuration;
    @Nullable
    private final Expression from;
    @Nullable
    private final Expression to;
    private final Expression timestamp;
    private final Expression stepOrBuckets;

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        description = """
            Creates groups of values - buckets - out of a `@timestamp` attribute using either a fixed step width
            or a target bucket count.
            Unlike [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md),
            which aligns buckets to calendar boundaries, TSTEP uses a fixed-width UTC grid anchored at the start
            of the query range. Each bucket is labeled by its right boundary.
            When a target bucket count is provided, TSTEP derives a fixed step width from the query range.
            The derived step is rounded up so that the result uses no more than the target number of buckets.

            When using ES|QL in Kibana, the range can be derived automatically from the
            [`@timestamp` filter](docs-content://explore-analyze/query-filter/languages/esql-kibana.md#_standard_time_filter)
            that Kibana adds to the query.""",
        examples = {
            @Example(
                description = """
                    Group `@timestamp` into one-hour buckets when the request includes a `@timestamp` filter:""",
                file = "tstep",
                tag = "docsTStepByOneHourDuration",
                explanation = """
                    The returned `bucket` values are the end timestamps of each bucket.
                    Boundaries are generated as `range_start + n * w` (UTC) for bucket width `w`, and each bucket
                    represents `(bucket_end - w, bucket_end]`."""
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
            Fixed bucket width on a UTC grid, or a target bucket count. When a bucket count is provided,
            the actual step width is derived from `from` and `to` and rounded up so the target bucket count
            is not exceeded.
            TSTEP always needs a range to anchor the grid; when `from` and `to` are omitted,
            the range is derived from the request `@timestamp` filter.""") Expression stepOrBuckets,
        @Param(
            name = "from",
            type = { "date", "date_nanos", "keyword" },
            description = "Start of the time range that anchors the step grid. Required together with `to`.",
            optional = true
        ) @Nullable Expression from,
        @Param(
            name = "to",
            type = { "date", "date_nanos", "keyword" },
            description = "End of the time range. Required together with `from`.",
            optional = true
        ) @Nullable Expression to,
        Expression timestamp,
        Configuration configuration
    ) {
        super(source, Bucket.fields(stepOrBuckets, timestamp, from, to));
        this.stepOrBuckets = stepOrBuckets;
        this.from = from;
        this.to = to;
        this.timestamp = timestamp;
        this.configuration = configuration;
    }

    public TStep(Source source, Expression stepOrBuckets, Expression timestamp, Configuration configuration) {
        this(source, stepOrBuckets, null, null, timestamp, configuration);
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
        // `ResolveTimestampBoundsAware` runs before `ImplicitCasting`
        // so a string literal like "1h" still has `KEYWORD` type.
        // Accept foldable `KEYWORD` so bounds are injected before postAnalysisVerification runs.
        return from == null
            && to == null
            && stepOrBuckets.resolved()
            && (stepOrBuckets.dataType() == DataType.TIME_DURATION
                || stepOrBuckets.dataType().isWholeNumber()
                || stepOrBuckets.dataType() == DataType.KEYWORD && stepOrBuckets.foldable());
    }

    @Override
    public Expression withTimestampBounds(Literal startBound, Literal endBound) {
        return new TStep(source(), stepOrBuckets, from != null ? from : startBound, to != null ? to : endBound, timestamp, configuration);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (stepOrBuckets.resolved() && (from == null || to == null)) {
            failures.add(
                Failure.fail(
                    this,
                    "[{}] requires either a `@timestamp` range in the request query filter" + " or explicit `from` and `to` parameters",
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
        var ctx = FoldContext.small();
        Expression step = stepOrBuckets.dataType() == DataType.TIME_DURATION
            ? stepOrBuckets
            : Literal.timeDuration(source(), Duration.ofMillis(stepToLong(ctx)));
        Literal tick = Literal.timeDuration(
            source(),
            timestamp.dataType() == DataType.DATE_NANOS ? Duration.ofNanos(1) : Duration.ofMillis(1)
        );
        Expression sub = new Sub(source(), timestamp, tick, configuration);
        Bucket bucket = new Bucket(source(), sub, step, null, null, configuration.withZoneId(ZoneOffset.UTC), offsetToLong(ctx));

        return new Add(source(), bucket, step, configuration);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        var resolution = isType(
            stepOrBuckets,
            dt -> dt == DataType.TIME_DURATION || dt == DataType.INTEGER || dt == DataType.LONG,
            sourceText(),
            FIRST,
            "time_duration",
            "integer",
            "long"
        ).and(isType(timestamp, DataType::isMillisOrNanos, sourceText(), IMPLICIT, "date_nanos or datetime"));
        if (resolution.unresolved()) {
            return resolution;
        }

        if (stepOrBuckets.dataType() == DataType.INTEGER || stepOrBuckets.dataType() == DataType.LONG) {
            FoldContext foldContext = FoldContext.small();
            if (from == null != (to == null)) {
                return new TypeResolution("[" + sourceText() + "] requires both 'from' and 'to' arguments, or neither");
            }

            if (from == null) {
                if (stepOrBuckets.foldable() == false) {
                    return new TypeResolution("[" + sourceText() + "] target bucket count must be a constant");
                }
                long count = ((Number) stepOrBuckets.fold(foldContext)).longValue();
                if (count <= 0 || count > Integer.MAX_VALUE) {
                    return new TypeResolution(
                        "[" + sourceText() + "] requires a bucket count between 1 and " + Integer.MAX_VALUE + ", got [" + count + "]"
                    );
                }
                return resolution;
            }

            var typeResolution = isStringOrDateBound(Objects.requireNonNull(from), SECOND).and(
                isStringOrDateBound(Objects.requireNonNull(to), THIRD)
            );
            if (typeResolution.unresolved()) {
                return typeResolution;
            }

            if (stepOrBuckets.foldable() == false) {
                return new TypeResolution("[" + sourceText() + "] target bucket count must be a constant");
            }

            long count = ((Number) stepOrBuckets.fold(foldContext)).longValue();
            if (count <= 0 || count > Integer.MAX_VALUE) {
                return new TypeResolution(
                    "[" + sourceText() + "] requires a bucket count between 1 and " + Integer.MAX_VALUE + ", got [" + count + "]"
                );
            }

            if (from.foldable() == false || to.foldable() == false) {
                return new TypeResolution("[" + sourceText() + "] `from` and `to` must be constant when using a target bucket count");
            }

            if (DataType.isString(from.dataType()) || DataType.isString(to.dataType())) {
                try {
                    temporalToLong(foldContext, Objects.requireNonNull(from));
                    temporalToLong(foldContext, Objects.requireNonNull(to));
                } catch (IllegalArgumentException e) {
                    return new TypeResolution("[" + sourceText() + "] cannot derive step from bucket count: " + e.getMessage());
                }
            }

            return typeResolution;
        }

        if (from == null != (to == null)) {
            return new TypeResolution("[" + sourceText() + "] requires both 'from' and 'to' arguments, or neither");
        }

        if (from == null) {
            return resolution;
        }

        return resolution.and(
            isStringOrDateBound(Objects.requireNonNull(from), SECOND).and(isStringOrDateBound(Objects.requireNonNull(to), THIRD))
        );
    }

    @Override
    public DataType dataType() {
        return timestamp.resolved() ? timestamp.dataType() : DataType.UNSUPPORTED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        Expression from = newChildren.size() > 2 ? newChildren.get(2) : null;
        Expression to = newChildren.size() > 3 ? newChildren.get(3) : null;
        return new TStep(source(), newChildren.get(0), from, to, newChildren.get(1), configuration);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TStep::new, stepOrBuckets, from, to, timestamp, configuration);
    }

    public Configuration configuration() {
        return configuration;
    }

    public Bucket timeBucketSpecRef() {
        FoldContext ctx = FoldContext.small();
        Expression step = stepOrBuckets.dataType() == DataType.TIME_DURATION
            ? stepOrBuckets
            : Literal.timeDuration(source(), Duration.ofMillis(stepToLong(ctx)));
        long offset = offsetToLong(ctx);
        return new Bucket(source(), timestamp, step, null, null, configuration.withZoneId(ZoneOffset.UTC), offset);
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return NAME + "{step=" + stepOrBuckets + ", from=" + from + ", to=" + to + "}";
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

    private long temporalToLong(FoldContext ctx, Expression temporal) {
        var folded = temporal.fold(ctx);
        if (DataType.isString(temporal.dataType())) {
            return dateTimeToLong(((BytesRef) folded).utf8ToString());
        }
        long value = ((Number) folded).longValue();
        return temporal.dataType() == DataType.DATE_NANOS ? DateUtils.toMilliSeconds(value) : value;
    }

    private long offsetToLong(FoldContext ctx) {
        long f = temporalToLong(ctx, Objects.requireNonNull(from));
        long s = stepToLong(ctx);
        return s > 0 ? DateUtils.floorRemainder(f, s) : 0L;
    }

    // TODO(sidosera): Consolidate T{STEP,BUCKET} bound validation.
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

    private long stepToLong(FoldContext ctx) {
        long f = temporalToLong(ctx, Objects.requireNonNull(from));
        long t = temporalToLong(ctx, Objects.requireNonNull(to));
        var folded = stepOrBuckets.fold(ctx);

        if (stepOrBuckets.dataType() == DataType.TIME_DURATION) {
            return ((Duration) folded).toMillis();
        }

        long range = Math.max(0L, t - f);
        if (range == 0L) {
            return 1L;
        }

        long count = ((Number) folded).longValue();
        long scaled = 1000L * count;
        if ((count <= Long.MAX_VALUE / 1000L) && (range >= scaled)) {
            // Prefer whole-second steps once the derived step reaches second-scale.
            return Math.ceilDiv(range, scaled) * 1000L;
        }
        // Keep millisecond precision for sub-second derived steps.
        return Math.ceilDiv(range, count);
    }

}
