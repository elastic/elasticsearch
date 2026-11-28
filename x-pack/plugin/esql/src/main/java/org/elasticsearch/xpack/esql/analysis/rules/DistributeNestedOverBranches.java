/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} read the nested documents of the rows of an index. The rows a {@code FORK} or a
 * subquery merge come from several branches, each reading its own index, so the merged rows have no nested documents to read.
 * Both act on one row at a time, so applying them to each branch gives the rows they give the merged rows. This moves such a
 * command, when it directly follows the merge, into every branch. Only the part that reads nested documents moves: the other
 * conjuncts of a {@code WHERE} and the other fields of an {@code EVAL} stay after the merge, where a column that some branches
 * lack is still null for them.
 */
public final class DistributeNestedOverBranches extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(UnaryPlan.class, unary -> {
            if (unary.child() instanceof MergePlan merge && readsNested(unary)) {
                return distribute(unary, merge);
            }
            return unary;
        });
    }

    private static LogicalPlan distribute(UnaryPlan unary, MergePlan merge) {
        if (unary instanceof Filter filter) {
            List<Expression> nested = Predicates.splitAnd(filter.condition())
                .stream()
                .filter(DistributeNestedOverBranches::callsNestedAny)
                .toList();
            List<Expression> rest = Predicates.splitAnd(filter.condition()).stream().filter(e -> callsNestedAny(e) == false).toList();
            LogicalPlan moved = intoBranches(new Filter(filter.source(), filter.child(), Predicates.combineAnd(nested)), merge);
            return rest.isEmpty() ? moved : new Filter(filter.source(), moved, Predicates.combineAnd(rest));
        }
        if (unary instanceof Eval eval) {
            List<Alias> nested = eval.fields().stream().filter(f -> callsNestedAny(f.child())).toList();
            List<Alias> rest = eval.fields().stream().filter(f -> callsNestedAny(f.child()) == false).toList();
            if (rest.isEmpty() == false && independent(nested, rest)) {
                LogicalPlan moved = intoBranches(new Eval(eval.source(), eval.child(), nested), merge);
                return new Eval(eval.source(), moved, rest);
            }
        }
        return intoBranches(unary, merge);
    }

    /** Whether no field of one list reads or redefines a name the other defines, so the two lists can run in either order. */
    private static boolean independent(List<Alias> first, List<Alias> second) {
        Set<String> firstNames = names(first);
        Set<String> secondNames = names(second);
        return reads(first).stream().noneMatch(secondNames::contains)
            && reads(second).stream().noneMatch(firstNames::contains)
            && firstNames.stream().noneMatch(secondNames::contains);
    }

    private static Set<String> names(List<Alias> fields) {
        Set<String> names = new HashSet<>();
        fields.forEach(f -> names.add(f.name()));
        return names;
    }

    private static Set<String> reads(List<Alias> fields) {
        Set<String> reads = new HashSet<>();
        fields.forEach(f -> f.child().forEachDown(UnresolvedAttribute.class, a -> reads.add(a.name())));
        return reads;
    }

    private static LogicalPlan intoBranches(UnaryPlan command, MergePlan merge) {
        return merge.replaceSubPlans(merge.children().stream().map(branch -> into(command, branch)).toList());
    }

    private static boolean readsNested(UnaryPlan plan) {
        if (plan instanceof NestedExpand expand) {
            return expand.bound() == false;
        }
        return (plan instanceof Filter || plan instanceof Eval)
            && plan.expressions().stream().anyMatch(DistributeNestedOverBranches::callsNestedAny);
    }

    private static boolean callsNestedAny(Expression expression) {
        return expression.anyMatch(e -> e instanceof UnresolvedFunction function && function.name().equalsIgnoreCase("nested_any"));
    }

    private static LogicalPlan into(UnaryPlan command, LogicalPlan branch) {
        if (branch instanceof Subquery subquery) {
            return subquery.replaceChild(command.replaceChild(subquery.child()));
        }
        return command.replaceChild(branch);
    }
}
