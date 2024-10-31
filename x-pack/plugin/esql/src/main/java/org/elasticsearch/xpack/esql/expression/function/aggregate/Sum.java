/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.OverflowingSumLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.OverflowingSumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.OverflowingSumLongGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.planner.ToIntermediateState;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

/**
 * Sum all values of a field in matching documents.
 */
public class Sum extends ConfigurationAggregateFunction implements ToAggregator, ToIntermediateState, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sum", Sum::new);

    @FunctionInfo(
        returnType = { "long", "double" },
        description = "The sum of a numeric expression.",
        isAggregation = true,
        examples = {
            @Example(file = "stats", tag = "sum"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate "
                    + "the sum of each employee's maximum salary changes, apply the "
                    + "`MV_MAX` function to each row and then sum the results",
                file = "stats",
                tag = "docsStatsSumNestedExpression"
            ) }
    )
    public Sum(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }) Expression field,
        Configuration configuration
    ) {
        this(source, field, Literal.TRUE, configuration);
    }

    public Sum(Source source, Expression field, Expression filter, Configuration configuration) {
        super(source, field, filter, emptyList(), configuration);
    }

    private Sum(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Sum> info() {
        return NodeInfo.create(this, Sum::new, field(), filter(), configuration());
    }

    @Override
    public Sum replaceChildren(List<Expression> newChildren) {
        return new Sum(source(), newChildren.get(0), newChildren.get(1), configuration());
    }

    @Override
    public Sum withFilter(Expression filter) {
        return new Sum(source(), field(), filter, configuration());
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
        );
    }

    @Override
    public DataType dataType() {
        DataType dt = field().dataType();
        return dt.isWholeNumber() == false || dt == UNSIGNED_LONG ? DOUBLE : LONG;
    }

    @Override
    public final AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (type == DataType.LONG) {
            // Old aggregator without overflow handling
            if (configuration().clusterHasFeature(EsqlFeatures.FN_SUM_OVERFLOW_HANDLING) == false) {
                return new OverflowingSumLongAggregatorFunctionSupplier(inputChannels);
            }
            var location = source().source();
            return new SumLongAggregatorFunctionSupplier(
                location.getLineNumber(),
                location.getColumnNumber(),
                source().text(),
                inputChannels
            );
        }
        if (type == DataType.INTEGER) {
            return new SumIntAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.DOUBLE) {
            return new SumDoubleAggregatorFunctionSupplier(inputChannels);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public List<IntermediateStateDesc> intermediateState(boolean grouping) {
        DataType type = field().dataType();
        if (type == DataType.LONG && configuration().clusterHasFeature(EsqlFeatures.FN_SUM_OVERFLOW_HANDLING) == false) {
            return grouping
                ? OverflowingSumLongGroupingAggregatorFunction.intermediateStateDesc()
                : OverflowingSumLongAggregatorFunction.intermediateStateDesc();
        }
        return null;
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        // SUM(const) is equivalent to MV_SUM(const)*COUNT(*).
        return field.foldable()
            ? new Mul(s, new MvSum(s, field), new Count(s, new Literal(s, StringUtils.WILDCARD, DataType.KEYWORD)))
            : null;
    }
}
