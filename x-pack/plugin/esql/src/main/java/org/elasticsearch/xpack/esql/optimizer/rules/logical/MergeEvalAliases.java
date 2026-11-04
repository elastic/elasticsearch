/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NonDeterministic;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merge the aliases of an {@link Eval} into the expressions that use them, so that a chain of dependent aliases is evaluated by a
 * single expression instead of one evaluator per alias:
 * <pre>{@code
 * Eval[[a + b AS sum, sum * c AS result, result - 1 AS result2]]
 * }</pre>
 * becomes
 * <pre>{@code
 * Eval[[((a + b) * c) - 1 AS result2]]
 * }</pre>
 * <p>
 * An alias is only merged when its single use is inside a later field of the same Eval. Merging an alias that is used more than
 * once, or that is also used outside the Eval, would make the plan evaluate its expression several times, which can be more
 * expensive than evaluating it once and reading the result back. For the same reason aliases defining a {@link NonDeterministic}
 * expression are never merged: the evaluations could disagree.
 * <p>
 * Aliases defined in different Evals are merged once {@link CombineEvals} has combined those Evals.
 */
public final class MergeEvalAliases extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        AttributeSet usedOutsideEvalFields = usedOutsideEvalFields(plan);
        return plan.transformUp(Eval.class, eval -> mergeAliases(eval, usedOutsideEvalFields));
    }

    /**
     * Collect the attributes that something other than the field expressions of an {@link Eval} depends on: the output of the
     * plan and the references of every node. {@link Eval#references()} does not include the aliases the Eval itself defines,
     * so uses of an alias by a later field of the same Eval are not part of this set.
     */
    private static AttributeSet usedOutsideEvalFields(LogicalPlan plan) {
        AttributeSet.Builder used = AttributeSet.builder();
        used.addAll(plan.output());
        plan.forEachDown(p -> {
            used.addAll(p.references());
            // references() is not guaranteed to account for every expression a node holds, e.g. the timestamp of a
            // TimeSeriesAggregate is missing from it. An attribute that is dropped while still in use is a broken plan, so also
            // take every attribute found in the expressions of the node. An Eval is excluded because it is handled field by field.
            if (p instanceof Eval == false) {
                p.forEachExpression(Attribute.class, used::add);
            }
            // The output of a merge is defined positionally from the output of its branches, not by referencing it.
            if (p instanceof MergePlan || p instanceof UnionAll) {
                p.children().forEach(child -> used.addAll(child.output()));
            }
            // A stub stands for the output of the left hand side of an inline join, without referencing it.
            if (p instanceof StubRelation) {
                used.addAll(p.output());
            }
        });
        return used.build();
    }

    private static Eval mergeAliases(Eval eval, AttributeSet usedOutsideEvalFields) {
        List<Alias> fields = eval.fields();
        if (fields.size() < 2) {
            return eval;
        }

        Map<NameId, Integer> usesInFields = new HashMap<>();
        for (Alias field : fields) {
            field.child().forEachDown(Attribute.class, a -> usesInFields.merge(a.id(), 1, Integer::sum));
        }

        // Fields are processed in order, so an alias merged into a later field carries the already merged expression of the
        // aliases it depends on.
        AttributeMap.Builder<Expression> mergeable = AttributeMap.builder();
        List<Alias> newFields = new ArrayList<>(fields.size());
        boolean changed = false;
        for (Alias field : fields) {
            Alias newField = field;
            AttributeMap<Expression> merge = mergeable.build();
            if (merge.isEmpty() == false) {
                Holder<Boolean> merged = new Holder<>(false);
                Expression newChild = field.child().transformUp(Attribute.class, a -> {
                    Expression replacement = merge.resolve(a, a);
                    if (replacement != a) {
                        merged.set(true);
                    }
                    return replacement;
                });
                if (merged.get()) {
                    newField = field.replaceChild(newChild);
                    changed = true;
                }
            }
            Attribute attribute = field.toAttribute();
            if (usesInFields.getOrDefault(attribute.id(), 0) == 1
                && usedOutsideEvalFields.contains(attribute) == false
                && newField.child().anyMatch(NonDeterministic.class::isInstance) == false) {
                // The alias has no other use, so drop it: relying on PruneColumns for that would leave the expression evaluated
                // twice wherever it does not reach, e.g. inside the branches of a UnionAll.
                mergeable.put(attribute, newField.child());
                changed = true;
            } else {
                newFields.add(newField);
            }
        }

        return changed ? new Eval(eval.source(), eval.child(), newFields) : eval;
    }
}
