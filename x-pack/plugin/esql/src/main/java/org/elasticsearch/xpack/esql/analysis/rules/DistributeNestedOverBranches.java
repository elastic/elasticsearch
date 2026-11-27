/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.core.Nullable;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} read the nested documents of the rows of an index. The rows a {@code FORK} or a
 * subquery merge come from several branches, each reading its own index, so the merged rows have no nested documents to read.
 * Both act on one row at a time, so applying them to each branch gives the rows they give the merged rows. This moves such a
 * command, when only {@code WHERE} and {@code EVAL} commands that do not read nested documents lie between it and the merge, into
 * every branch. Only the part that reads nested documents moves: the other conjuncts of a {@code WHERE} and the other fields of an
 * {@code EVAL} stay after the merge, where a column that some branches lack is still null for them.
 */
public final class DistributeNestedOverBranches extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(UnaryPlan.class, unary -> {
            if (readsNested(unary) == false) {
                return unary;
            }
            List<UnaryPlan> between = new ArrayList<>();
            LogicalPlan below = unary.child();
            while (below instanceof Filter || below instanceof Eval) {
                UnaryPlan command = (UnaryPlan) below;
                if (command.expressions().stream().anyMatch(DistributeNestedOverBranches::callsNestedAny)) {
                    return unary;
                }
                between.add(command);
                below = command.child();
            }
            return below instanceof MergePlan merge ? distribute(unary, between, merge) : unary;
        });
    }

    /** The part of a command that reads nested documents, and what is left of it for after the merge. */
    private record Split(UnaryPlan moved, @Nullable UnaryPlan rest) {}

    private static LogicalPlan distribute(UnaryPlan unary, List<UnaryPlan> between, MergePlan merge) {
        Split split = split(unary);
        Set<String> defined = new HashSet<>();
        for (UnaryPlan command : between) {
            if (command instanceof Eval eval) {
                eval.fields().forEach(f -> defined.add(f.name()));
            }
        }
        Set<String> reads = new HashSet<>();
        split.moved().expressions().forEach(e -> e.forEachDown(UnresolvedAttribute.class, a -> reads.add(a.name())));
        if (reads.stream().anyMatch(defined::contains)) {
            return unary;
        }
        LogicalPlan result = intoBranches(split.moved(), merge);
        for (int i = between.size() - 1; i >= 0; i--) {
            result = between.get(i).replaceChild(result);
        }
        return split.rest() == null ? result : split.rest().replaceChild(result);
    }

    private static Split split(UnaryPlan unary) {
        if (unary instanceof Filter filter) {
            List<Expression> conjuncts = Predicates.splitAnd(filter.condition());
            List<Expression> nested = conjuncts.stream().filter(DistributeNestedOverBranches::callsNestedAny).toList();
            List<Expression> rest = conjuncts.stream().filter(e -> callsNestedAny(e) == false).toList();
            Filter moved = new Filter(filter.source(), filter.child(), Predicates.combineAnd(nested));
            return new Split(moved, rest.isEmpty() ? null : new Filter(filter.source(), filter.child(), Predicates.combineAnd(rest)));
        }
        if (unary instanceof Eval eval) {
            List<Alias> nested = eval.fields().stream().filter(f -> callsNestedAny(f.child())).toList();
            List<Alias> rest = eval.fields().stream().filter(f -> callsNestedAny(f.child()) == false).toList();
            if (rest.isEmpty() == false && independent(nested, rest)) {
                return new Split(new Eval(eval.source(), eval.child(), nested), new Eval(eval.source(), eval.child(), rest));
            }
        }
        return new Split(unary, null);
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
