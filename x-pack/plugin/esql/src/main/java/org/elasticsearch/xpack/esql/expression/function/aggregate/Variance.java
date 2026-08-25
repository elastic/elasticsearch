/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Variance extends AggregateFunction implements ToAggregator, NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Variance", Variance::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Variance.class)
        .unary(Variance::new)
        .name("variance", "std_var");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // PromQL requires IEEE-754 semantics, so the across-series variance reports NaN for non-finite input.
        .acrossSeries((source, field) -> new Variance(source, field, true))
        .description("Calculates the population variance across the input vector.")
        .example("stdvar(http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("stdvar");

    /**
     * When {@code true}, a non-finite aggregation result is reported as {@code NaN} instead of {@code null}. Set only by
     * the PromQL translation; native ES|QL {@code VARIANCE}/{@code STD_VAR} uses the default strict (finite-only) form.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "double",
        briefSummary = "Returns the population variance of a numeric field.",
        description = "The population variance of a numeric field.",
        type = FunctionType.AGGREGATE,
        examples = { @Example(file = "stats", tag = "variance") }
    )
    public Variance(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, false);
    }

    /**
     * Builds a {@code Variance} that reports non-finite results as {@code NaN} when {@code allowNonFinite} is true.
     * Used by the PromQL translation; native ES|QL {@code VARIANCE}/{@code STD_VAR} uses the strict (finite-only) form.
     */
    public Variance(Source source, Expression field, boolean allowNonFinite) {
        this(source, field, Literal.TRUE, NO_WINDOW, allowNonFinite);
    }

    public Variance(Source source, Expression field, Expression filter, Expression window) {
        this(source, field, filter, window, false);
    }

    public Variance(Source source, Expression field, Expression filter, Expression window, boolean allowNonFinite) {
        super(source, field, filter, window, emptyList());
        this.allowNonFinite = allowNonFinite;
    }

    private Variance(StreamInput in) throws IOException {
        super(in);
        this.allowNonFinite = NonFiniteSupport.readNonFinite(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // The non-finite flag, when present, follows the base fields; version-gated so older nodes never see the byte.
        super.writeTo(out);
        writeNonFinite(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Variance(source(), field(), filter(), window(), false);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
        );
    }

    @Override
    protected NodeInfo<Variance> info() {
        return NodeInfo.create(this, Variance::new, field(), filter(), window(), allowNonFinite);
    }

    @Override
    public Variance replaceChildren(List<Expression> newChildren) {
        return new Variance(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), allowNonFinite);
    }

    public Variance withFilter(Expression filter) {
        return new Variance(source(), field(), filter, window(), allowNonFinite);
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.LONG) {
            return new StdDevLongAggregatorFunctionSupplier(false, allowNonFinite);
        }
        if (type == DataType.INTEGER) {
            return new StdDevIntAggregatorFunctionSupplier(false, allowNonFinite);
        }
        if (type == DataType.DOUBLE) {
            return new StdDevDoubleAggregatorFunctionSupplier(false, allowNonFinite);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
