/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

/**
 * PromQL {@code sort} / {@code sort_desc}: identity-preserving ordering of an instant vector by sample value.
 * Histogram dropping is applied at translation (see {@link #translate}); the actual {@code OrderBy} is injected above
 * {@code TimeSeriesCollapse} by the result-ordering analyzer rule.
 */
public final class SortFunction extends PromqlFunctionCall implements ResultOrderingFunction {
    public SortFunction(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, SortFunction::new, child(), definition(), parameters());
    }

    @Override
    public SortFunction replaceChild(LogicalPlan newChild) {
        return new SortFunction(source(), newChild, definition(), parameters());
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.RESULT_ORDERING;
    }

    @Override
    public boolean isIdentityTransparent() {
        return true;
    }

    /**
     * Identity translation: {@code sort}/{@code sort_desc} preserve the child's schema and grain, except native-histogram
     * samples are dropped (Prometheus {@code filterFloats}). Ordering is injected later.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        IntermediateResult result = context.translate(child());
        if (result.value().resolved() && result.value().dataType().isHistogram()) {
            var skipAllFilter = new Filter(source(), result.plan(), Literal.FALSE);
            var nullGrouping = new Values(source(), new Literal(source(), null, DataType.DOUBLE));
            IntermediateResult skipped = result.with(skipAllFilter, result.schema(), result.value());
            return skipped.kind().afterInitialAggregation
                ? context.regroup(skipped, result.schema(), false, nullGrouping)
                : context.collapse(skipped, result.schema(), nullGrouping);
        }
        return result;
    }

    @Override
    public ResultOrdering resultOrdering(List<Attribute> commandOutput, Configuration configuration) {
        Attribute value = commandOutput.getFirst();
        if (definition().name().equals("sort_desc")) {
            String keyName = Attribute.rawTemporaryName("promql_sort", "nan");
            Alias nanKey = new Alias(source(), keyName, new NotEquals(source(), value, value), null, true);
            return new ResultOrdering(
                List.of(nanKey),
                List.of(
                    new Order(source(), nanKey.toAttribute(), Order.OrderDirection.ASC, Order.NullsPosition.LAST),
                    new Order(source(), value, Order.OrderDirection.DESC, Order.NullsPosition.LAST)
                )
            );
        }
        return new ResultOrdering(List.of(), List.of(new Order(source(), value, Order.OrderDirection.ASC, Order.NullsPosition.LAST)));
    }
}
