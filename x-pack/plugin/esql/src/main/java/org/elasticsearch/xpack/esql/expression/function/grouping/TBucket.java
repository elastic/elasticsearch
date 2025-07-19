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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.LocalSurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc.maybeSubstituteWithRoundTo;
import static org.elasticsearch.xpack.esql.session.Configuration.DEFAULT_TZ;

/**
 * Splits dates and numbers into a given number of buckets. There are two ways to invoke
 * this function: with a user-provided span (explicit invocation mode), or a span derived
 * from a number of desired buckets (as a hint) and a range (auto mode).
 * In the former case, two parameters will be provided, in the latter four.
 */
public class TBucket extends GroupingFunction.EvaluatableGroupingFunction
    implements
        PostOptimizationVerificationAware,
        TwoOptionalArguments,
        LocalSurrogateExpression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "TBucket", TBucket::new);

    private final Expression field;
    private final Expression buckets;

    @FunctionInfo(
        returnType = { "double", "date", "date_nanos" },
        description = """
            Creates groups of values - buckets - out of a datetime or numeric input.
            The size of the buckets can either be provided directly, or chosen based on a recommended count and values range.""",
        examples = {},
        type = FunctionType.GROUPING
    )
    public TBucket(
        Source source,
        @Param(
            name = "field",
            type = { "integer", "long", "double", "date", "date_nanos" },
            description = "Numeric or date expression from which to derive buckets."
        ) Expression field,
        @Param(
            name = "buckets",
            type = { "integer", "long", "double", "date_period", "time_duration" },
            description = "Target number of buckets, or desired bucket size if `from` and `to` parameters are omitted."
        ) Expression buckets
    ) {
        super(source, List.of(field, buckets));
        this.field = field;
        this.buckets = buckets;
    }

    private TBucket(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(buckets);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Rounding.Prepared preparedRounding = getDateRounding(toEvaluator.foldCtx(), null, null);
        return DateTrunc.evaluator(field.dataType(), source(), toEvaluator.apply(field), preparedRounding);
    }

    /**
     * Returns the date rounding from this bucket function if the target field is a date type; otherwise, returns null.
     */
    public Rounding.Prepared getDateRoundingOrNull(FoldContext foldCtx) {
        return getDateRounding(foldCtx, null, null);
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
        return isType(buckets, DataType::isTemporalAmount, sourceText(), SECOND, "date_period", "time_duration");
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        String operation = sourceText();
        failures.add(isFoldable(buckets, operation, SECOND));
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TBucket(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TBucket::new, field, buckets);
    }

    public Expression field() {
        return field;
    }

    public Expression buckets() {
        return buckets;
    }

    @Override
    public String toString() {
        return "Bucket{" + "field=" + field + ", buckets=" + buckets + "}";
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
