/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Splits dates into a given number of buckets. The span is derived from a time range provided.
 */
public class TBucket extends GroupingFunction.EvaluatableGroupingFunction implements SurrogateExpression {
    public static final String NAME = "TBucket";

    private final Expression buckets;
    private final Expression timestamp;

    @FunctionInfo(
        returnType = { "date", "date_nanos" },
        description = """
            Creates groups of values - buckets - out of a @timestamp attribute. The size of the buckets must be provided directly.""",
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
        type = FunctionType.GROUPING
    )
    public TBucket(
        Source source,
        @Param(name = "buckets", type = { "date_period", "time_duration" }, description = "Desired bucket size.") Expression buckets
    ) {
        this(source, buckets, new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD));
    }

    public TBucket(Source source, Expression buckets, Expression timestamp) {
        super(source, List.of(buckets, timestamp));
        this.buckets = buckets;
        this.timestamp = timestamp;
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
        return new Bucket(source(), timestamp, buckets, null, null);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(buckets, DataType::isTemporalAmount, sourceText(), DEFAULT, "date_period", "time_duration").and(
            isType(timestamp, dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS, sourceText(), SECOND, "date_nanos or datetime")
        );
    }

    @Override
    public DataType dataType() {
        return timestamp.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TBucket(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TBucket::new, buckets, timestamp);
    }

    public Expression field() {
        return timestamp;
    }

    public Expression buckets() {
        return buckets;
    }

    @Override
    public String toString() {
        return "TBucket{buckets=" + buckets + "}";
    }
}
