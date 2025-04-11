/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SubstituteSurrogateAggregations extends OptimizerRules.OptimizerRule<Aggregate> {
    public SubstituteSurrogateAggregations() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        var aggs = aggregate.aggregates();
        List<NamedExpression> newAggs = new ArrayList<>(aggs.size());
        // existing aggregate and their respective attributes
        Map<AggregateFunction, Attribute> aggFuncToAttr = new HashMap<>();
        // surrogate functions eval
        List<Alias> transientEval = new ArrayList<>();
        boolean changed = false;

        // first pass to check existing aggregates (to avoid duplication and alias waste)
        for (NamedExpression agg : aggs) {
            if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                if ((af instanceof SurrogateExpression se && se.surrogate() != null) == false) {
                    aggFuncToAttr.put(af, agg.toAttribute());
                }
            }
        }

        int[] counter = new int[] { 0 };
        // 0. check list of surrogate expressions
        for (NamedExpression agg : aggs) {
            Expression e = Alias.unwrap(agg);
            if (e instanceof SurrogateExpression sf && sf.surrogate() != null) {
                changed = true;
                Expression s = sf.surrogate();

                // if the expression is NOT a 1:1 replacement need to add an eval
                if (s instanceof AggregateFunction == false) {
                    // 1. collect all aggregate functions from the expression
                    var surrogateWithRefs = s.transformUp(AggregateFunction.class, af -> {
                        if (af instanceof TimeSeriesAggregateFunction) {
                            return af;
                        }
                        // 2. check if they are already use otherwise add them to the Aggregate with some made-up aliases
                        // 3. replace them inside the expression using the given alias
                        var attr = aggFuncToAttr.get(af);
                        // the agg doesn't exist in the Aggregate, create an alias for it and save its attribute
                        if (attr == null) {
                            var temporaryName = TemporaryNameUtils.temporaryName(af, agg, counter[0]++);
                            // create a synthetic alias (so it doesn't clash with a user defined name)
                            var newAlias = new Alias(agg.source(), temporaryName, af, null, true);
                            attr = newAlias.toAttribute();
                            aggFuncToAttr.put(af, attr);
                            newAggs.add(newAlias);
                        }
                        return attr;
                    });
                    // 4. move the expression as an eval using the original alias
                    // copy the original alias id so that other nodes using it down stream (e.g. eval referring to the original agg)
                    // don't have to updated
                    var aliased = new Alias(agg.source(), agg.name(), surrogateWithRefs, agg.toAttribute().id());
                    transientEval.add(aliased);
                }
                // the replacement is another aggregate function, so replace it in place
                else {
                    newAggs.add((NamedExpression) agg.replaceChildren(Collections.singletonList(s)));
                }
            } else {
                newAggs.add(agg);
            }
        }

        LogicalPlan plan = aggregate;
        if (changed) {
            var source = aggregate.source();
            if (newAggs.isEmpty() == false) {
                plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
            } else {
                // All aggs actually have been surrogates for (foldable) expressions, e.g.
                // \_Aggregate[[],[AVG([1, 2][INTEGER]) AS s]]
                // Replace by a local relation with one row, followed by an eval, e.g.
                // \_Eval[[MVAVG([1, 2][INTEGER]) AS s]]
                // \_LocalRelation[[{e}#21],[ConstantNullBlock[positions=1]]]
                plan = new LocalRelation(
                    source,
                    List.of(new EmptyAttribute(source)),
                    LocalSupplier.of(new Block[] { BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, null, 1) })
                );
            }
            // 5. force the initial projection in place
            if (transientEval.isEmpty() == false) {
                plan = new Eval(source, plan, transientEval);
                // project away transient fields and re-enforce the original order using references (not copies) to the original aggs
                // this works since the replaced aliases have their nameId copied to avoid having to update all references (which has
                // a cascading effect)
                plan = new Project(source, plan, Expressions.asAttributes(aggs));
            }
        }

        return plan;
    }
}
