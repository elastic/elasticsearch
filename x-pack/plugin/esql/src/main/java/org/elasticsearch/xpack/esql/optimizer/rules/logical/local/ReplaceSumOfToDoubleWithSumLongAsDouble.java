/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumLongAsDouble;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces {@code SUM(TO_DOUBLE(x))} over a {@code long} {@code x} with {@link SumLongAsDouble}, which adds up the {@code long}s
 * themselves instead of converting each one to a {@code double} first and running a Kahan summation over them:
 * <pre>{@code
 *   Aggregate[[], [SUM($$sum, ...) AS s, COUNT(x) AS c]]
 *   \_Eval[[TODOUBLE(x) AS $$sum]]
 *     \_EsRelation[...]
 * }</pre>
 * becomes
 * <pre>{@code
 *   Aggregate[[], [SUMLONGASDOUBLE(x) AS s, COUNT(x) AS c]]
 *   \_Eval[[TODOUBLE(x) AS $$sum]]     <-- unused, dropped by the rules that prune unused columns
 *     \_EsRelation[...]
 * }</pre>
 * That is the shape of the {@code AVG} of a {@code long}, which {@link org.elasticsearch.xpack.esql.expression.function.aggregate.Avg}
 * rewrites to {@code SUM(TO_DOUBLE(x)) / COUNT(x)}.
 * <p>
 *     This is a local, data node, rule on purpose. {@link SumLongAsDouble} has the intermediate state of {@link Sum} of a
 *     {@code double}, and the coordinator plans, and merges the partial results of, the original {@link Sum}. So no node needs
 *     to know about the replacement but the one that applies it, and mixed version clusters keep working without a transport
 *     version. The result differs from the one of the original plan only in the last digits, and only when the sum exceeds
 *     what a {@code double} holds exactly: this one is rounded once, instead of once per value.
 * </p>
 * <p>
 *     Only ungrouped, unfiltered and not windowed sums with the default, compensated, summation mode are replaced, and
 *     only if the {@code long} is read as is: anything that isn't an attribute would have to be evaluated in an
 *     {@link org.elasticsearch.xpack.esql.plan.logical.Eval} anyway.
 * </p>
 */
public class ReplaceSumOfToDoubleWithSumLongAsDouble extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        // The grouping variant of the aggregator function isn't implemented.
        if (aggregate.groupings().isEmpty() == false || aggregate instanceof TimeSeriesAggregate) {
            return aggregate;
        }

        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        aggregate.child().forEachUp(p -> p.forEachExpression(Alias.class, a -> aliasesBuilder.put(a.toAttribute(), a.child())));
        AttributeMap<Expression> aliases = aliasesBuilder.build();

        boolean changed = false;
        List<NamedExpression> newAggregates = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression aggregateExpression : aggregate.aggregates()) {
            if (aggregateExpression instanceof Alias alias && alias.child() instanceof Sum sum) {
                Attribute longAttribute = longSummedAsDouble(aggregate, sum, aliases);
                if (longAttribute != null) {
                    newAggregates.add(alias.replaceChild(new SumLongAsDouble(sum.source(), longAttribute)));
                    changed = true;
                    continue;
                }
            }
            newAggregates.add(aggregateExpression);
        }
        return changed ? aggregate.with(aggregate.groupings(), newAggregates) : aggregate;
    }

    /**
     * The {@code long} attribute {@code x} if {@code sum} is a plain {@code SUM(TO_DOUBLE(x))}, otherwise {@code null}.
     */
    private static Attribute longSummedAsDouble(Aggregate aggregate, Sum sum, AttributeMap<Expression> aliases) {
        if (sum.isSimpleSum() == false || SummationMode.fromLiteral(sum.summationMode()) != SummationMode.COMPENSATED) {
            return null;
        }
        Expression summed = aliases.resolve(sum.field(), sum.field());
        if (summed instanceof ToDouble toDouble && toDouble.field() instanceof Attribute attribute && attribute.dataType() == DataType.LONG
        // The sum reads the attribute itself now, so it has to be there for the aggregate to read.
            && aggregate.child().outputSet().contains(attribute)) {
            return attribute;
        }
        return null;
    }
}
