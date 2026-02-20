/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.IMPLICIT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Splits dates into buckets based on the {@code @timestamp} field.
 * <p>
 * The {@code buckets} parameter works like {@link Bucket}: if it's a number, it's the target number of buckets;
 * if it's a duration or period, it's the explicit bucket size.
 * <p>
 * When using a target number of buckets, start/end bounds are needed and can be provided explicitly
 * as {@code from}/{@code to} parameters.
 */
public class TBucket extends GroupingFunction.EvaluatableGroupingFunction
    implements
        SurrogateExpression,
        TimestampAware,
        TwoOptionalArguments,
        ConfigurationFunction {
    public static final String NAME = "TBucket";

    private final Configuration configuration;
    private final Expression buckets;
    private final Expression timestamp;
    @Nullable
    private final Expression from;
    @Nullable
    private final Expression to;

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        description = """
            Creates groups of values - buckets - out of a @timestamp attribute. The size of the buckets can either be provided \
            directly as a duration or period, or chosen based on a target bucket count \
            from explicit `from`/`to` parameters {applies_to}`stack: ga 9.4`.""",
        examples = {
            @Example(description = """
                Provide a bucket size as an argument.""", file = "tbucket", tag = "docsTBucketByOneHourDuration", explanation = """
                ::::{note}
                When providing the bucket size, it must be a time duration or date period.
                Also the reference is epoch, which starts at `0001-01-01T00:00:00Z`.
                ::::"""),
            @Example(
                description = """
                    Provide a string representation of bucket size as an argument.""",
                file = "tbucket",
                tag = "docsTBucketByOneHourDurationAsString",
                explanation = """
                    ::::{note}
                    When providing the bucket size, it can be a string representation of time duration or date period.
                    For example, "1 hour". Also the reference is epoch, which starts at `0001-01-01T00:00:00Z`.
                    ::::"""
            ) },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") },
        type = FunctionType.GROUPING
    )
    public TBucket(
        Source source,
        @Param(
            name = "buckets",
            type = { "integer", "date_period", "time_duration" },
            description = "Target number of buckets, or desired bucket size. "
                + "When a number, the actual bucket size is derived from `from`/`to` {applies_to}`stack: ga 9.4`. "
                + "When a duration or period, it is the explicit bucket size."
        ) Expression buckets,
        @Param(
            name = "from",
            type = { "date", "keyword", "text" },
            optional = true,
            description = "Start of the range. Required with a numeric `buckets` {applies_to}`stack: ga 9.4`."
        ) @Nullable Expression from,
        @Param(
            name = "to",
            type = { "date", "keyword", "text" },
            optional = true,
            description = "End of the range. Required with a numeric `buckets` {applies_to}`stack: ga 9.4`."
        ) @Nullable Expression to,
        Expression timestamp,
        Configuration configuration
    ) {
        super(source, Bucket.fields(buckets, timestamp, from, to));
        this.buckets = buckets;
        this.timestamp = timestamp;
        this.from = from;
        this.to = to;
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
        if (buckets.resolved() && buckets.dataType().isWholeNumber()) {
            if (from == null || to == null) {
                return null;
            }
            return new Bucket(source(), timestamp, buckets, from, to, configuration);
        }
        return new Bucket(source(), timestamp, buckets, from, to, configuration);
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
        if (resolution.unresolved()) {
            return resolution;
        }
        if (buckets.dataType().isWholeNumber() && (from == null || to == null)) {
            return new TypeResolution("numeric bucket count in [" + sourceText() + "] requires [from] and [to] parameters");
        }
        if (from == null != (to == null)) {
            return new TypeResolution("[from] and [to] in [" + sourceText() + "] must both be provided or both omitted");
        }
        return resolution;
    }

    private static TypeResolution isStringOrDate(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, exp -> DataType.isString(exp) || DataType.isDateTime(exp), operationName, paramOrd, "datetime", "string");
    }

    @Override
    public DataType dataType() {
        return timestamp.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        Expression from = newChildren.size() > 2 ? newChildren.get(2) : null;
        Expression to = newChildren.size() > 3 ? newChildren.get(3) : null;
        return new TBucket(source(), newChildren.get(0), from, to, newChildren.get(1), configuration);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TBucket::new, buckets, from, to, timestamp, configuration);
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

    @Override
    public String toString() {
        return "TBucket{buckets=" + buckets + ", from=" + from + ", to=" + to + "}";
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
