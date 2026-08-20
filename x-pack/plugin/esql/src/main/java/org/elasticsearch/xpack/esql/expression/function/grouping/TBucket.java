/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.ThreeOptionalArguments;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.IMPLICIT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.function.grouping.Bucket.isStringOrDate;

/**
 * Splits dates into buckets based on the {@code @timestamp} field.
 * <p>
 * The {@code buckets} parameter works like {@link Bucket}: if it's a number, it's the target number of buckets;
 * if it's a duration or period, it's the explicit bucket size.
 * <p>
 * When using a target number of buckets, start/end bounds are needed and can be provided explicitly
 * as {@code from}/{@code to} parameters or derived automatically from the query DSL {@code @timestamp} range filter
 * via {@link TimestampBoundsAware}.
 */
public class TBucket extends GroupingFunction.EvaluatableGroupingFunction
    implements
        OnlySurrogateExpression,
        TimestampAware,
        ThreeOptionalArguments,
        TimestampBoundsAware.OfExpression,
        ConfigurationFunction,
        AnyNullIsNull {
    public static final String NAME = "TBucket";

    private static final String OPTIONS_APPLIES_TO = """
        {"serverless": "ga", "stack": "ga 9.6.0"}""";
    private static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(Bucket.INCLUDE_EMPTY_BUCKETS, DataType.BOOLEAN);

    private final Configuration configuration;
    private final Expression buckets;
    private final Expression timestamp;
    @Nullable
    private final Expression from;
    @Nullable
    private final Expression to;
    @Nullable
    private final Expression options;

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TBucket.class)
        .quinaryConfigWithOptions(TBucket::new)
        .name("tbucket");

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        briefSummary = "Creates timestamp-based buckets aligned to calendar boundaries.",
        description = """
            Creates groups of values - buckets - out of a `@timestamp` attribute.
            The size of the buckets can be provided directly as a duration or period.
            Alternatively, the bucket size can be chosen based on a recommended count
            and a range {applies_to}`stack: ga 9.4`.

            When using ES|QL in Kibana, the range can be derived automatically from the
            [`@timestamp` filter](docs-content://explore-analyze/query-filter/languages/esql-kibana.md#_standard_time_filter)
            that Kibana adds to the query.""",
        examples = {
            @Example(
                description = """
                    `TBUCKET` can work in two modes: one in which the size of the bucket is computed
                    based on a buckets count recommendation and a range, and
                    another in which the bucket size is provided directly as a duration or period.

                    Using a target number of buckets, a start of a range, and an end of a range,
                    `TBUCKET` picks an appropriate bucket size to generate the target number of buckets or fewer.
                    For example, asking for at most 3 buckets over a 2 hour range results in hourly buckets
                    {applies_to}`stack: ga 9.4`:""",
                file = "tbucket",
                tag = "tbucketWithNumericBucketsAndExplicitFromToCount",
                explanation = """
                    The goal isn't to provide **exactly** the target number of buckets,
                    it's to pick a range that people are comfortable with that provides at most the target number of buckets."""
            ),
            @Example(
                description = """
                    Asking for more buckets can result in a finer granularity.
                    For example, asking for at most 20 buckets in the same range results in 10-minute buckets
                    {applies_to}`stack: ga 9.4`:""",
                file = "tbucket",
                tag = "tbucketWithNumericBucketsAndExplicitFromToCountFinerGranularity",
                explanation = """
                    ::::{note}
                    `TBUCKET` does not filter any rows. It only uses the provided range to pick a good bucket size.
                    For rows with a value outside of the range, it returns a bucket value that corresponds to a bucket
                    outside the range. Combine `TBUCKET` with [`WHERE`](/reference/query-languages/esql/commands/where.md)
                    to filter rows.
                    ::::"""
            ),
            @Example(description = """
                If the desired bucket size is known in advance, simply provide it as the first argument,
                leaving the range out:""", file = "tbucket", tag = "docsTBucketByOneHourDuration", explanation = """
                ::::{note}
                When providing the bucket size, it must be a time duration or date period.
                Also the reference is epoch, which starts at `0001-01-01T00:00:00Z`.
                ::::"""),
            @Example(
                description = "The bucket size can also be provided as a string:",
                file = "tbucket",
                tag = "docsTBucketByOneHourDurationAsString"
            ) },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") },
        type = FunctionType.GROUPING
    )
    public TBucket(
        Source source,
        @Param(
            name = "buckets",
            type = { "integer", "date_period", "time_duration" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Target number of buckets, or desired bucket size. "
                + "When a number is provided, the actual bucket size is derived from `from`/`to` "
                + "or the `@timestamp` range in the query filter {applies_to}`stack: ga 9.4`. "
                + "When a duration or period is provided, it is used as the explicit bucket size."
        ) Expression buckets,
        @Param(
            name = "from",
            type = { "date", "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            optional = true,
            description = "Start of the range. Required with a numeric `buckets` when no `@timestamp` range is in the "
                + "query filter {applies_to}`stack: ga 9.4`."
        ) @Nullable Expression from,
        @Param(
            name = "to",
            type = { "date", "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            optional = true,
            description = "End of the range. Required with a numeric `buckets` when no `@timestamp` range is in the "
                + "query filter {applies_to}`stack: ga 9.4`."
        ) @Nullable Expression to,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = Bucket.INCLUDE_EMPTY_BUCKETS,
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "When `true`, empty buckets (filled with default aggregate values) are output across the "
                        + "`from`..`to` range (`from` inclusive, `to` exclusive). Requires the four-argument (range) form of `BUCKET`. "
                        + "Defaults to `false`.",
                    applies_to = OPTIONS_APPLIES_TO
                ) },
            description = "(Optional) Additional options as <<esql-function-named-params,function named parameters>>.",
            optional = true,
            applies_to = OPTIONS_APPLIES_TO
        ) @Nullable Expression options,
        Expression timestamp,
        Configuration configuration
    ) {
        super(source, Bucket.fields(buckets, timestamp, from, to, options));
        this.buckets = buckets;
        this.timestamp = timestamp;
        this.from = from;
        this.to = to;
        this.options = options;
        this.configuration = configuration;
    }

    public TBucket(
        Source source,
        Expression buckets,
        @Nullable Expression from,
        @Nullable Expression to,
        Expression timestamp,
        Configuration configuration
    ) {
        this(source, buckets, from, to, null, timestamp, configuration);
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
        return buckets.resolved() && buckets.dataType().isWholeNumber() && from == null && to == null;
    }

    @Override
    public Expression withTimestampBounds(Literal start, Literal end) {
        return new TBucket(source(), buckets, from != null ? from : start, to != null ? to : end, options, timestamp, configuration);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (buckets.resolved() && buckets.dataType().isWholeNumber() && (from == null || to == null)) {
            failures.add(
                Failure.fail(
                    this,
                    "numeric bucket count in [{}] requires [from] and [to] parameters or a `@timestamp` range in the query filter",
                    sourceText()
                )
            );
        }
    }

    @Override
    public Expression surrogate() {
        if (buckets.resolved() && buckets.dataType().isWholeNumber()) {
            assert from != null && to != null
                : "numeric bucket count requires [from] and [to]; postAnalysisVerification should have caught this";
            return new Bucket(source(), timestamp, buckets, from, to, options, configuration);
        }
        return new Bucket(source(), timestamp, buckets, from, to, options, configuration);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution timestampResolution = isType(timestamp, DataType::isMillisOrNanos, sourceText(), IMPLICIT, "date_nanos or datetime");
        TypeResolution bucketsResolution = isType(
            buckets,
            dt -> dt.isWholeNumber() || DataType.isTemporalAmount(dt),
            sourceText(),
            DEFAULT,
            "integer",
            "date_period",
            "time_duration"
        );
        TypeResolution resolution = bucketsResolution.and(timestampResolution);
        if (from != null) {
            resolution = resolution.and(isStringOrDate(from, sourceText(), SECOND));
        }
        if (to != null) {
            resolution = resolution.and(isStringOrDate(to, sourceText(), THIRD));
        }
        if (options != null) {
            resolution = resolution.and(Options.resolve(options, source(), FOURTH, ALLOWED_OPTIONS));
        }
        if (resolution.unresolved()) {
            return resolution;
        }
        if (DataType.isTemporalAmount(buckets.dataType()) && (from != null || to != null)) {
            return new TypeResolution("[from] and [to] in [" + sourceText() + "] cannot be used with a duration or period bucket size");
        }
        if (from == null != (to == null)) {
            return new TypeResolution("[from] and [to] in [" + sourceText() + "] must both be provided or both omitted");
        }
        if (includeEmptyBuckets() && (from == null || to == null)) {
            return new TypeResolution("[" + Bucket.INCLUDE_EMPTY_BUCKETS + "] in [" + sourceText() + "] requires [from] and [to]");
        }
        return resolution;
    }

    @Override
    public DataType dataType() {
        return timestamp.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        int i = 0;
        Expression newBuckets = newChildren.get(i++);
        Expression newTimestamp = newChildren.get(i++);
        Expression newFrom = from != null ? newChildren.get(i++) : null;
        Expression newTo = to != null ? newChildren.get(i++) : null;
        Expression newOptions = options != null ? newChildren.get(i++) : null;
        return new TBucket(source(), newBuckets, newFrom, newTo, newOptions, newTimestamp, configuration);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TBucket::new, buckets, from, to, options, timestamp, configuration);
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    public Expression buckets() {
        return buckets;
    }

    public Expression from() {
        return from;
    }

    public Expression to() {
        return to;
    }

    public Expression options() {
        return options;
    }

    public boolean includeEmptyBuckets() {
        if (options == null) {
            return false;
        }
        Map<String, Object> map = new HashMap<>();
        Options.populateMap((MapExpression) options, map, source(), FOURTH, ALLOWED_OPTIONS);
        return Boolean.TRUE.equals(map.get(Bucket.INCLUDE_EMPTY_BUCKETS));
    }

    @Override
    public String toString() {
        return "TBucket{buckets=" + buckets + ", from=" + from + ", to=" + to + ", options=" + options + "}";
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
        TBucket other = (TBucket) obj;

        return configuration.equals(other.configuration);
    }
}
