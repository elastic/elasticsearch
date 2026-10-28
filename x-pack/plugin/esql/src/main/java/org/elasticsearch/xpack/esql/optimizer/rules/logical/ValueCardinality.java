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
     * The cardinality of {@code attribute} as seen in the output of {@code plan}.
     */
    static ValueCardinality of(Attribute attribute, LogicalPlan plan) {
        return ofAttribute(attribute, plan, 0);
    }

    private static ValueCardinality ofAttribute(Attribute attribute, LogicalPlan plan, int depth) {
        if (depth > MAX_DEPTH) {
            return MAYBE_MULTI;
        }
        // Each case either finds the producer of the attribute or hands the question to the child. Plan nodes that are not
        // listed may rewrite columns, so they end the search.
        if (plan instanceof Eval eval) {
            Alias definition = definitionOf(attribute, eval.fields());
            // A field may read the fields before it in the same Eval, so its definition is resolved against this Eval, not its child.
            return definition != null ? ofExpression(definition.child(), eval, depth + 1) : ofAttribute(attribute, eval.child(), depth + 1);
        }
        if (plan instanceof Row row) {
            Alias definition = definitionOf(attribute, row.fields());
            return definition != null ? ofExpression(definition.child(), plan, depth + 1) : MAYBE_MULTI;
        }
        if (plan instanceof Project project) {
            for (NamedExpression projection : project.projections()) {
                if (projection.id().equals(attribute.id())) {
                    return projection instanceof Alias alias
                        ? ofExpression(alias.child(), project.child(), depth + 1)
                        : ofAttribute(attribute, project.child(), depth + 1);
                }
            }
            return MAYBE_MULTI;
        }
        if (plan instanceof MvExpand mvExpand) {
            return mvExpand.expanded().id().equals(attribute.id()) ? SINGLE : ofAttribute(attribute, mvExpand.child(), depth + 1);
        }
        if (plan instanceof Aggregate aggregate) {
            return ofAggregateOutput(attribute, aggregate);
        }
        if (plan instanceof EsRelation) {
            // A field is single-valued only if the mapping of every resolved index says so. A field that may be read from _source or
            // converted is not a plain mapped field, so its EsField never carries the flag.
            return attribute instanceof FieldAttribute field && field.field().isStaticallySingleValued() ? SINGLE : MAYBE_MULTI;
        }
        if (plan instanceof Filter || plan instanceof Limit || plan instanceof OrderBy) {
            return ofAttribute(attribute, plan.children().get(0), depth + 1);
        }
        return MAYBE_MULTI;
    }

    private static ValueCardinality ofExpression(Expression expression, LogicalPlan plan, int depth) {
        if (depth > MAX_DEPTH) {
            return MAYBE_MULTI;
        }
        if (isScalarFoldable(expression)) {
            return SINGLE;
        }
        if (expression instanceof Alias alias) {
            return ofExpression(alias.child(), plan, depth + 1);
        }
        if (expression instanceof Attribute attribute) {
            return ofAttribute(attribute, plan, depth + 1);
        }
        if (expression instanceof MvMin
            || expression instanceof MvMax
            || expression instanceof MvFirst
            || expression instanceof MvLast
            || expression instanceof MvCount) {
            return SINGLE;
        }
        return MAYBE_MULTI;
    }

    private static ValueCardinality ofAggregateOutput(Attribute attribute, Aggregate aggregate) {
        for (NamedExpression output : aggregate.aggregates()) {
            if (output.id().equals(attribute.id()) == false) {
                // skip columns that aren't the attribute we are looking for
                continue;
            }
            Expression unwrapped = Alias.unwrap(output);
            if (unwrapped instanceof Count) {
                return SINGLE;
            }
            // Other aggregate functions can return a list (VALUES, TOP, ...), so only the key itself is proven. A grouping
            // key holds one value per group even when the input column was a list, because the aggregation unrolls the list.
            return isGroupKey(aggregate, output) ? SINGLE : MAYBE_MULTI;
        }
        return MAYBE_MULTI;
    }

    private static boolean isGroupKey(Aggregate aggregate, NamedExpression output) {
        Expression unwrappedOutput = Alias.unwrap(output);
        for (Expression grouping : aggregate.groupings()) {
            if (unwrappedOutput.semanticEquals(Alias.unwrap(grouping))
                || (grouping instanceof NamedExpression named && named.id().equals(output.id()))) {
                return true;
            }
        }
        return false;
    }

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
