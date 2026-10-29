/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFirst;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLast;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.util.Collection;
import java.util.List;

/**
 * Whether an attribute is <em>definitely</em> single-valued: on every row it holds one value or {@code null}, never a list.
 * <p>
 * The data type does not say so, because every ES|QL type may hold a list. Optimizer rewrites that are only valid for
 * single-valued inputs (for example recomputing a {@code STATS BY} key above the aggregation) need a proof, and {@link #of}
 * is the one place that gives it. An answer is {@link #SINGLE} only when a rule below proves it; everything else, including
 * every plan shape this class does not know, is {@link #MAYBE_MULTI}. A caller must treat {@code MAYBE_MULTI} as "unknown", not
 * as "multi-valued".
 * <p>
 * The proof follows the attribute from {@code plan} down to the node that produced it, and stops at the first of:
 * <ul>
 *   <li>a scalar literal or constant expression;</li>
 *   <li>the output of {@code MV_MIN}, {@code MV_MAX}, {@code MV_FIRST}, {@code MV_LAST} or {@code MV_COUNT}, which reduce any
 *       input to at most one value;</li>
 *   <li>the attribute {@link MvExpand} produces;</li>
 *   <li>a grouping key or a {@code COUNT} of an earlier {@link Aggregate}; and</li>
 *   <li>an {@link Eval}, {@link Project} or {@code ROW} alias of an attribute proven by these rules.</li>
 * </ul>
 * {@code SINGLE} means <em>statically</em> single-valued: it follows from the query and from what the source guarantees, without
 * reading data, so it holds on every shard. A source field is {@code SINGLE} only if its source guarantees a single value, as recorded
 * in {@link org.elasticsearch.xpack.esql.core.type.EsField#isStaticallySingleValued()}. The segment statistics of {@code SearchStats}
 * are not used: they describe one shard and exist only in local planning. A source that does not set that flag answers
 * {@code MAYBE_MULTI}.
 * <p>
 * "Single-valued" is what the ES|QL field loaders return. A value that the loaders do not read, such as one that is kept only in
 * {@code _source}, does not count. See {@link org.elasticsearch.xpack.esql.core.type.EsField#isStaticallySingleValued()} for how
 * that applies to an index.
 */
enum ValueCardinality {
    SINGLE,
    MAYBE_MULTI;

    /**
     * Longest chain of aliases and plan nodes to follow. A longer chain answers {@link #MAYBE_MULTI}, which keeps the recursion
     * within the stack for queries that spell out thousands of chained aliases.
     */
    private static final int MAX_DEPTH = ExpressionBuilder.MAX_EXPRESSION_DEPTH;

    /**
     * Whether {@code expression} is single-valued at {@code plan}: the node, or stage, of the pipeline that the expression is evaluated
     * at, meaning the pipeline from the source up to that node. The columns that the expression reads must be outputs of that node, so
     * for an expression that belongs to a node, such as a grouping of a {@code STATS}, pass the child of the node.
     */
    static ValueCardinality of(Expression expression, LogicalPlan plan) {
        return ofExpression(expression, plan, 0);
    }

    /**
     * Whether {@code expression}, written in {@code node}, is single-valued. Picks the plan to pass to {@link #of} from the type of the
     * node, so the caller does not have to. The answer is {@link #MAYBE_MULTI} for a node that is not listed.
     */
    static ValueCardinality ofWrittenIn(Expression expression, LogicalPlan node) {
        if (node instanceof Eval || node instanceof Row) {
            // These can read their own earlier fields, so evaluate over the node itself: EVAL a = MV_MAX(languages), k = a
            return of(expression, node);
        }
        if (node instanceof Aggregate
            || node instanceof Filter
            || node instanceof OrderBy
            || node instanceof Project
            || node instanceof Limit
            || node instanceof TopN) {
            // These read the columns that their child outputs.
            return of(expression, node.children().get(0));
        }
        return MAYBE_MULTI;
    }

    /**
     * Follows {@code attribute} down {@code plan} to the node that produced it. In the examples, the key of the last {@code STATS} is
     * the attribute that is asked about.
     */
    private static ValueCardinality ofAttribute(Attribute attribute, LogicalPlan plan, int depth) {
        if (depth > MAX_DEPTH) {
            return MAYBE_MULTI;
        }
        // Each case finds the producer of the attribute, or passes the question to the child.
        if (plan instanceof Eval eval) {
            Alias definition = definitionOf(attribute, eval.fields());
            if (definition != null) {
                // Defined here. Resolve against this EVAL, since the expression may read earlier fields of it.
                // Example: FROM test | EVAL a = MV_MAX(languages), k = a | STATS BY k
                return ofExpression(definition.child(), eval, depth + 1);
            } else {
                // Passed through from below.
                return ofAttribute(attribute, eval.child(), depth + 1);
            }
        }
        if (plan instanceof Row row) {
            // Nothing is below a ROW, so a column that it does not define is unknown.
            Alias definition = definitionOf(attribute, row.fields());
            if (definition != null) {
                // Example: ROW x = [1, 2] | STATS BY x -> a multi-valued literal, so unknown
                return ofExpression(definition.child(), plan, depth + 1);
            } else {
                return MAYBE_MULTI;
            }
        }
        if (plan instanceof Project project) {
            for (NamedExpression projection : project.projections()) {
                if (projection.id().equals(attribute.id())) {
                    if (projection instanceof Alias alias) {
                        // A rename. Resolve its source against the child, not this node: projections do not read each other
                        // (RENAME a AS b, b AS a).
                        // Example: FROM test | EVAL a = MV_MAX(languages) | RENAME a AS k | STATS BY k
                        return ofExpression(alias.child(), project.child(), depth + 1);
                    } else {
                        // A kept column keeps its id.
                        return ofAttribute(attribute, project.child(), depth + 1);
                    }
                }
            }
            // Not an output of this node.
            return MAYBE_MULTI;
        }
        if (plan instanceof MvExpand mvExpand) {
            if (mvExpand.expanded().id().equals(attribute.id())) {
                // The expanded column has one value per row.
                return SINGLE;
            } else {
                return ofAttribute(attribute, mvExpand.child(), depth + 1);
            }
        }
        if (plan instanceof Aggregate aggregate) {
            // An earlier STATS. See ofAggregateOutput.
            return ofAggregateOutput(attribute, aggregate);
        }
        if (plan instanceof EsRelation) {
            // The source: single-valued only if the mapping of every resolved index guarantees it. A field that may be read from
            // _source or converted is not a plain mapped field, so its EsField never carries the flag.
            return attribute instanceof FieldAttribute field && field.field().isStaticallySingleValued() ? SINGLE : MAYBE_MULTI;
        }
        if (plan instanceof Filter || plan instanceof Limit || plan instanceof OrderBy || plan instanceof TopN) {
            // These keep, drop or reorder rows without changing columns. The optimizer makes a TopN from SORT followed by LIMIT.
            return ofAttribute(attribute, plan.children().get(0), depth + 1);
        }
        // Any other node, such as LOOKUP JOIN or GROK, may rewrite columns, so nothing is known.
        return MAYBE_MULTI;
    }

    /**
     * The same question for the right-hand side of a definition, such as {@code MV_MAX(languages)} in {@code EVAL a = MV_MAX(languages)},
     * with its columns resolved against {@code plan}.
     */
    private static ValueCardinality ofExpression(Expression expression, LogicalPlan plan, int depth) {
        if (depth > MAX_DEPTH) {
            return MAYBE_MULTI;
        }
        if (isScalarFoldable(expression)) {
            // A constant folds to null or one value, but a multi-valued literal such as [1, 2] folds to a list.
            return SINGLE;
        }
        if (expression instanceof Alias alias) {
            return ofExpression(alias.child(), plan, depth + 1);
        }
        if (expression instanceof Attribute attribute) {
            // A copy of another column.
            return ofAttribute(attribute, plan, depth + 1);
        }
        if (expression instanceof MvMin
            || expression instanceof MvMax
            || expression instanceof MvFirst
            || expression instanceof MvLast
            || expression instanceof MvCount) {
            // These turn any input, even a list, into at most one value.
            return SINGLE;
        }
        // Any other function is unknown. Even a function of a single value can return a list: SPLIT.
        return MAYBE_MULTI;
    }

    /**
     * The same question for an output of an earlier {@code STATS}: an aggregate function or a grouping key.
     */
    private static ValueCardinality ofAggregateOutput(Attribute attribute, Aggregate aggregate) {
        for (NamedExpression output : aggregate.aggregates()) {
            if (output.id().equals(attribute.id()) == false) {
                // Skip columns that aren't the attribute we are looking for.
                continue;
            }
            Expression unwrapped = Alias.unwrap(output);
            if (unwrapped instanceof Count) {
                // One number per group.
                return SINGLE;
            }
            // Other aggregate functions can return a list (VALUES, TOP, ...), so only the key itself is proven. A grouping
            // key holds one value per group even when the input column was a list, because the aggregation unrolls the list.
            // Example: FROM test | STATS v = VALUES(languages) BY first_name | STATS n = COUNT(*) BY v -> unknown
            return isGroupKey(aggregate, output) ? SINGLE : MAYBE_MULTI;
        }
        return MAYBE_MULTI;
    }

    /**
     * Whether {@code output} is one of the grouping keys of {@code aggregate}, rather than an aggregate function.
     */
    private static boolean isGroupKey(Aggregate aggregate, NamedExpression output) {
        Expression unwrappedOutput = Alias.unwrap(output);
        for (Expression grouping : aggregate.groupings()) {
            // BY x + 1: the output is an alias of the grouping. BY languages: the same column, with the same id.
            if (unwrappedOutput.semanticEquals(Alias.unwrap(grouping))
                || (grouping instanceof NamedExpression named && named.id().equals(output.id()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * The {@code EVAL} or {@code ROW} field that defines {@code attribute}, or {@code null}.
     */
    private static Alias definitionOf(Attribute attribute, List<Alias> fields) {
        for (Alias field : fields) {
            if (field.id().equals(attribute.id())) {
                return field;
            }
        }
        return null;
    }

    private static boolean isScalarFoldable(Expression expression) {
        return expression.foldable() && expression.fold(FoldContext.small()) instanceof Collection<?> == false;
    }
}
