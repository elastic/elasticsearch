/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.LocalSurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc.maybeSubstituteWithRoundTo;
import static org.elasticsearch.xpack.esql.session.Configuration.DEFAULT_TZ;

/**
 * Splits dates into a given number of buckets. The span is derived from a time range provided.
 */
public class TBucket extends GroupingFunction.EvaluatableGroupingFunction
    implements
        PostOptimizationVerificationAware,
        TwoOptionalArguments,
        LocalSurrogateExpression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "TBucket", TBucket::new);

    private final Expression timestamp;
    private final Expression buckets;

    @FunctionInfo(
        returnType = { "date" },
        description = """
            Creates groups of values - buckets - out of a @timestamp attribute. The size of the buckets must be provided directly.""",
        examples = { @Example(description = """
            Provide a bucket size as an argument.""", file = "tbucket", tag = "docsTBucketByTimeDuration", explanation = """
            ::::{note}
            When providing the bucket size, it must be a time duration or date period.
            Also the reference is epoch, which starts at `0001-01-01T00:00:00Z`.
            ::::""") },
        type = FunctionType.GROUPING
    )
    public TBucket(
        Source source,
        @Param(name = "buckets", type = { "date_period", "time_duration" }, description = "Desired bucket size.") Expression buckets
    ) {
        this(source, new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD), buckets);
    }

    public TBucket(Source source, Expression timestamp, Expression buckets) {
        super(source, List.of(timestamp, buckets));
        this.timestamp = timestamp;
        this.buckets = buckets;
    }

    private TBucket(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(timestamp);
        out.writeNamedWriteable(buckets);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean foldable() {
        return timestamp.foldable() && buckets.foldable();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Rounding.Prepared preparedRounding = getDateRounding(toEvaluator.foldCtx(), null, null);
        return DateTrunc.evaluator(timestamp.dataType(), source(), toEvaluator.apply(timestamp), preparedRounding);
    }

    private Rounding.Prepared getDateRounding(FoldContext foldContext, Long min, Long max) {
        assert DataType.isTemporalAmount(buckets.dataType()) : "Unexpected span data type [" + buckets.dataType() + "]";
        return DateTrunc.createRounding(buckets.fold(foldContext), DEFAULT_TZ, min, max);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(buckets, DataType::isTemporalAmount, sourceText(), DEFAULT, "date_period", "time_duration");
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        String operation = sourceText();
        failures.add(isFoldable(buckets, operation, DEFAULT));
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
        return NodeInfo.create(this, TBucket::new, timestamp, buckets);
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

    @Override
    public Expression surrogate(SearchStats searchStats) {
        // LocalSubstituteSurrogateExpressions should make sure this doesn't happen
        assert searchStats != null : "SearchStats cannot be null";
        return maybeSubstituteWithRoundTo(
            source(),
            field(),
            buckets(),
            searchStats,
            (interval, minValue, maxValue) -> getDateRounding(FoldContext.small(), minValue, maxValue)
        );
    }
}
