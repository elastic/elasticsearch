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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
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
 * Whether an expression is <em>definitely</em> single-valued: on every row it holds one value or {@code null}, never multiple values.
 * <p>
 * This is useful for optimizer rewrites that are only valid for single-valued expressions. {@link #SINGLE} means definitely
 * single-valued. {@link #MAYBE_MULTI} means unknown: the expression is not proven single-valued, but it is not known to be
 * multi-valued either.
 * <p>
 * The answer is found by recursing through the plan. The recursion returns {@code SINGLE} when it finds:
 * <ul>
 *   <li>a scalar literal;</li>
 *   <li>the output of {@code MV_MIN}, {@code MV_MAX}, {@code MV_FIRST}, {@code MV_LAST} or {@code MV_COUNT}, which reduce any
 *       input to at most one value;</li>
 *   <li>the attribute {@link MvExpand} produces;</li>
 *   <li>a grouping key or a {@code COUNT} of an earlier {@link Aggregate};</li>
 *   <li>an {@link Eval}, {@link Project} or {@code ROW} alias of an attribute proven by these rules; and</li>
 *   <li>a source field whose mapping guarantees a single value, see
 *       {@link org.elasticsearch.xpack.esql.core.type.EsField#isStaticallySingleValued()}.</li>
 * </ul>
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
     * Whether {@code expression} is single-valued as {@code node} reads it.
     * <p>
     * This is a helper for calling {@link #ofOutput}. Rules should usually prefer it, because it is easier to use. Both methods are
     * needed because the same column can be single-valued at one point of a pipeline and not at another. For example, consider
     * {@code k} in the query {@code ... | EVAL k = ? | STATS COUNT(*) BY k}. There are two questions we might ask:
     * <ol>
     *   <li>Is {@code k}, as the {@code STATS} outputs it, single-valued? Yes, always. A group key holds one value per group, because
     *       the aggregation unrolls a list into one group per value. {@link #ofOutput} asks this.</li>
     *   <li>Is {@code k}, as the {@code STATS} reads it, single-valued? This depends on what the {@code EVAL} before the
     *       {@code STATS} does. A rewrite that recomputes values from the key needs this answer. {@code ofReadBy} asks this.</li>
     * </ol>
     * A node usually reads the output of its child. {@code EVAL} and {@code ROW} are the exception. A field can read an earlier field
     * of the same node, as in {@code EVAL a = MV_MAX(x), k = a}, so these nodes also read their own earlier fields. For them
     * {@code ofReadBy} uses the output of the node itself, so {@code ofReadBy} and {@link #ofOutput} give the same answer. For the
     * other nodes, use {@code ofOutput} to ask about a column that the node itself outputs, such as a group key of a {@code STATS}.
     */
    static ValueCardinality ofReadBy(Expression expression, LogicalPlan node) {
        if (node instanceof Eval || node instanceof Row) {
            // These can read their own earlier fields, so evaluate over the node itself: EVAL a = MV_MAX(languages), k = a
            return ofOutput(expression, node);
        }
        if (node instanceof Aggregate
            || node instanceof Filter
            || node instanceof OrderBy
            || node instanceof Project
            || node instanceof Limit
            || node instanceof TopN) {
            // These read the columns that their child outputs.
            return ofOutput(expression, node.children().get(0));
        }
        return MAYBE_MULTI;
    }

    /**
     * Whether {@code expression} is single-valued in the output of {@code plan}. Rules should generally use {@link #ofReadBy}, which
     * picks the plan for them. This method is mainly for tests, which can ask about the output of any node directly.
     * <p>
     * What a node outputs and what it reads can give different answers for the same column. For a group key of a {@code STATS}, the
     * output is always single-valued, but what the {@code STATS} reads may not be. See {@link #ofReadBy}.
     */
    static ValueCardinality ofOutput(Expression expression, LogicalPlan plan) {
        return ofExpression(expression, plan, 0);
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
            }
            // Passed through from below.
            return askChild(attribute, plan, depth + 1);
        }
        if (plan instanceof Row row) {
            // A ROW has no child, so every column it outputs is defined here.
            Alias definition = definitionOf(attribute, row.fields());
            if (definition != null) {
                // Example: ROW x = [1, 2] | STATS BY x -> a multi-valued literal, so unknown
                return ofExpression(definition.child(), plan, depth + 1);
            }
            // Only if the caller asks about a column that this node does not output.
            assert false : "column [" + attribute.name() + "] is not an output of this Row";
            return MAYBE_MULTI;
        }
        if (plan instanceof Project project) {
            for (NamedExpression projection : project.projections()) {
                if (projection.id().equals(attribute.id())) {
                    if (projection instanceof Alias alias) {
                        // A rename, so resolve its source against the child.
                        // Projections do not read each other (RENAME a AS b, b AS a), so we do not need the current node.
                        // Example: FROM test | EVAL a = MV_MAX(languages) | RENAME a AS k | STATS BY k
                        return ofExpression(alias.child(), project.child(), depth + 1);
                    }
                    // A kept column, so go to its definition below.
                    return askChild(attribute, plan, depth + 1);
                }
            }
            // Only if the caller asks about a column that this node does not output.
            assert false : "column [" + attribute.name() + "] is not an output of this Project";
            return MAYBE_MULTI;
        }
        if (plan instanceof MvExpand mvExpand) {
            if (mvExpand.expanded().id().equals(attribute.id())) {
                // The expanded column has one value per row.
                return SINGLE;
            }
            // Any other column comes from the child.
            return askChild(attribute, plan, depth + 1);
        }
        if (plan instanceof Aggregate aggregate) {
            // An earlier STATS.
            return ofAggregateOutput(attribute, aggregate);
        }
        if (plan instanceof EsRelation) {
            // The source: single-valued only if the mapping of every resolved index guarantees it. A field that may be read from
            // _source or converted is not a plain mapped field, so its EsField never carries the flag.
            return attribute instanceof FieldAttribute field && field.field().isStaticallySingleValued() ? SINGLE : MAYBE_MULTI;
        }
        if (plan instanceof Filter || plan instanceof Limit || plan instanceof OrderBy || plan instanceof TopN) {
            // These keep, drop or reorder rows without changing columns.
            return askChild(attribute, plan, depth + 1);
        }
        // Any other node, such as LOOKUP JOIN or GROK, may rewrite columns, so nothing is known.
        return MAYBE_MULTI;
    }

    /**
     * Passes the question about {@code attribute} to the only child of {@code plan}, for a node that does not change the column.
     */
    private static ValueCardinality askChild(Attribute attribute, LogicalPlan plan, int depth) {
        return ofAttribute(attribute, plan.children().get(0), depth);
    }

    /**
     * The same question for the right-hand side of a definition, such as {@code MV_MAX(languages)} in {@code EVAL a = MV_MAX(languages)},
     * with its columns resolved against {@code plan}.
     */
    private static ValueCardinality ofExpression(Expression expression, LogicalPlan plan, int depth) {
        if (depth > MAX_DEPTH) {
            return MAYBE_MULTI;
        }
        if (isScalarLiteral(expression)) {
            // A scalar literal, or null, is one value. A multi-valued literal such as [1, 2] holds a list.
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
            // These return at most one value, even for a list.
            return SINGLE;
        }

        // Any other function is unknown. Even a function of a single value can return a list, for example SPLIT.
        // TODO add more functions that are known to return single values.
        return MAYBE_MULTI;
    }

    /**
     * The same question for an output of an earlier {@code STATS}: a grouping key or an aggregate.
     */
    private static ValueCardinality ofAggregateOutput(Attribute attribute, Aggregate aggregate) {
        NamedExpression output = aggregate.aggregates().stream().filter(o -> o.id().equals(attribute.id())).findFirst().orElse(null);
        if (output == null) {
            // Only if the caller asks about a column that this node does not output.
            assert false : "column [" + attribute.name() + "] is not an output of this Aggregate";
            return MAYBE_MULTI;
        }

        if (isGroupKey(aggregate, output)) {
            // A grouping key holds one value per group even when the input column was a list, because the aggregation unrolls the list.
            return SINGLE;
        }

        Expression unwrapped = Alias.unwrap(output);
        if (unwrapped instanceof AggregateFunction) {
            if (unwrapped instanceof Count) {
                return SINGLE;
            }
            // TODO support other aggregate functions.
            // Some can return a list (VALUES, TOP, ...).
            return MAYBE_MULTI;
        }

        // Not an aggregate function. On a plan that is not optimized yet, an output can be an expression over aggregates, such as
        // COUNT(*) + 1, or a filtered aggregate.
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

    /**
     * Only a {@link Literal} counts, not any foldable expression. Folding an expression evaluates it, which can add warnings to the
     * response, as {@code 1 / 0} does. The optimizer folds constants before the rules that use this class, so a constant is a literal
     * by then. An expression that is not folded yet is {@link #MAYBE_MULTI}.
     */
    private static boolean isScalarLiteral(Expression expression) {
        return expression instanceof Literal literal && literal.value() instanceof Collection<?> == false;
    }
}
