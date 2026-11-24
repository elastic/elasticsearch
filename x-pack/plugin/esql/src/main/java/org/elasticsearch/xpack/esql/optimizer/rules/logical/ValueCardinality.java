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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
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
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
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
 * The answer is found by walking down the plan. The walk returns {@code SINGLE} when it finds:
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
     * Whether {@code expression} is single-valued in the output of {@code plan}: the columns that it reads are columns that
     * {@code plan} outputs.
     * <p>
     * Choosing {@code plan} needs care, because the same column can be single-valued at one point of a pipeline and multi-valued at
     * another. For example, consider {@code k} in the query {@code ... | EVAL k = ? | STATS COUNT(*) BY k}. There are two questions
     * we could ask:
     * <ol>
     *   <li>Is {@code k}, as the {@code STATS} reads it, single-valued? This depends on what the {@code EVAL} before the
     *       {@code STATS} outputs, so pass the child of the {@code STATS}. A rewrite that recomputes values from the key needs this
     *       answer.</li>
     *   <li>Is {@code k}, as the {@code STATS} outputs it, single-valued? Yes, a group key always has one value per group. Pass the
     *       {@code STATS} itself.</li>
     * </ol>
     * The columns that a node reads are usually the output of its child; this is not the case for {@code EVAL} and {@code ROW}. In
     * these nodes a field can read an earlier field of the same node, as in {@code EVAL a = MV_MAX(x), k = a}. To ask about an
     * expression of one of these nodes, pass the node itself.
     */
    static ValueCardinality ofOutput(Expression expression, LogicalPlan plan) {
        // A loop, not recursion: each step has at most one next step, so the walk needs no stack and no depth limit. It ends,
        // because each step either moves to the child of plan, or to an earlier field of the same EVAL or ROW.
        // An EVAL or ROW field can only read earlier fields of the same node. After the walk takes the field at index i of
        // fieldsOwner, it only needs to search the fields before i. This keeps a long chain of aliases linear, not quadratic.
        LogicalPlan fieldsOwner = null;
        int fieldsEnd = 0;
        while (true) {
            expression = Alias.unwrap(expression);
            if (expression instanceof Attribute == false) {
                return ofComputedValue(expression);
            }
            Attribute attribute = (Attribute) expression;

            // A copy of another column. Follow it down the plan to the node that produced it. Each case finds the producer of the
            // attribute, or passes the question to the child. In the examples, the key of the last STATS is the attribute that is
            // asked about.
            switch (plan) {
                case Eval eval -> {
                    int index = indexOfDefinition(attribute, eval.fields(), fieldsOwner == eval ? fieldsEnd : eval.fields().size());
                    if (index >= 0) {
                        // Defined here. Resolve against this EVAL, since the expression may read earlier fields of it.
                        // Example: FROM test | EVAL a = MV_MAX(languages), k = a | STATS BY k
                        expression = eval.fields().get(index).child();
                        fieldsOwner = eval;
                        fieldsEnd = index;
                    } else {
                        plan = eval.child();
                    }
                }
                case Row row -> {
                    // A ROW has no child, so every column it outputs is defined here.
                    int index = indexOfDefinition(attribute, row.fields(), fieldsOwner == row ? fieldsEnd : row.fields().size());
                    if (index < 0) {
                        return notAnOutput(attribute, row);
                    }
                    // Example: ROW x = [1, 2] | STATS BY x -> a multi-valued literal, so unknown
                    expression = row.fields().get(index).child();
                    fieldsOwner = row;
                    fieldsEnd = index;
                }
                case Project project -> {
                    NamedExpression projection = projectionOf(attribute, project);
                    if (projection == null) {
                        return notAnOutput(attribute, project);
                    }
                    if (projection instanceof Alias alias) {
                        // A rename, so resolve its source against the child.
                        // Projections do not read each other (RENAME a AS b, b AS a), so we do not need the current node.
                        // Example: FROM test | EVAL a = MV_MAX(languages) | RENAME a AS k | STATS BY k
                        expression = alias.child();
                    }
                    // A kept column, or the source of a rename, is defined below.
                    plan = project.child();
                }
                case MvExpand mvExpand -> {
                    if (mvExpand.expanded().id().equals(attribute.id())) {
                        // The expanded column has one value per row.
                        return SINGLE;
                    }
                    // Any other column comes from the child.
                    plan = mvExpand.child();
                }
                case Aggregate aggregate -> {
                    // An earlier STATS.
                    return ofAggregateOutput(attribute, aggregate);
                }
                case EsRelation relation -> {
                    // The source: single-valued only if the mapping of every resolved index guarantees it. A field that may be read
                    // from _source or converted is not a plain mapped field, so its EsField never carries the flag.
                    return attribute instanceof FieldAttribute field && field.field().isStaticallySingleValued() ? SINGLE : MAYBE_MULTI;
                }
                // These keep, drop or reorder rows without changing columns.
                case Filter filter -> plan = filter.child();
                case Limit limit -> plan = limit.child();
                case LimitBy limitBy -> plan = limitBy.child();
                case Sample sample -> plan = sample.child();
                case OrderBy orderBy -> plan = orderBy.child();
                case TopN topN -> plan = topN.child();
                case GeneratingPlan<?> generating -> {
                    // Nodes such as DISSECT, GROK and ENRICH add columns and keep the columns of the child.
                    // EVAL is also a GeneratingPlan, but it is handled above, since it can prove its own fields.
                    if (isGenerated(attribute, generating)) {
                        return MAYBE_MULTI;
                    }
                    plan = plan.children().get(0);
                }
                // Any other node, such as LOOKUP JOIN or INLINE STATS, may rewrite columns, so nothing is known. LogicalPlan is not
                // sealed, so these nodes cannot be listed.
                default -> {
                    return MAYBE_MULTI;
                }
            }
        }
    }

    /**
     * The same question for an output of an earlier {@code STATS}: a grouping key or an aggregate.
     */
    private static ValueCardinality ofAggregateOutput(Attribute attribute, Aggregate aggregate) {
        NamedExpression output = aggregate.aggregates().stream().filter(o -> o.id().equals(attribute.id())).findFirst().orElse(null);
        if (output == null) {
            return notAnOutput(attribute, aggregate);
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
     * <p>
     * Matches an output that is the attribute of a grouping, or an alias of that attribute. For example,
     * {@code BY k = languages + 1} outputs the attribute {@code k}, whereas, after optimization, {@code BY l = languages} outputs the
     * alias {@code l = languages}.
     * <p>
     * ValueCardinality does not support INLINE STATS yet. If support is added, this method must be extended, because
     * {@code INLINE STATS ... BY k = languages + 1} outputs the grouping alias {@code k = languages + 1}, not the attribute
     * {@code k}.
     */
    private static boolean isGroupKey(Aggregate aggregate, NamedExpression output) {
        if (Alias.unwrap(output) instanceof Attribute outputAttribute) {
            for (Expression grouping : aggregate.groupings()) {
                Attribute groupingAttribute = Expressions.attribute(grouping);
                if (groupingAttribute != null && outputAttribute.id().equals(groupingAttribute.id())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The answer when the caller asks about a column that {@code plan} does not output. That is a bug in the caller, so it fails in
     * tests. In production the answer is MAYBE_MULTI, which is always safe.
     */
    private static ValueCardinality notAnOutput(Attribute attribute, LogicalPlan plan) {
        assert false : "column [" + attribute.name() + "] is not an output of this " + plan.nodeName();
        return MAYBE_MULTI;
    }

    /**
     * The index of the {@code EVAL} or {@code ROW} field before {@code end} that defines {@code attribute}, or {@code -1}. It searches
     * backwards, since a field most often reads the field just before it.
     */
    private static int indexOfDefinition(Attribute attribute, List<Alias> fields, int end) {
        for (int i = end - 1; i >= 0; i--) {
            if (fields.get(i).id().equals(attribute.id())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The projection of {@code project} that outputs {@code attribute}, or {@code null}.
     */
    private static NamedExpression projectionOf(Attribute attribute, Project project) {
        for (NamedExpression projection : project.projections()) {
            if (projection.id().equals(attribute.id())) {
                return projection;
            }
        }
        return null;
    }

    /**
     * Whether {@code generating} adds {@code attribute}, rather than passing it through from its child.
     */
    private static boolean isGenerated(Attribute attribute, GeneratingPlan<?> generating) {
        for (Attribute generated : generating.generatedAttributes()) {
            if (generated.id().equals(attribute.id())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The answer for an expression that is not a copy of a column, so it does not depend on the cardinality of another column.
     */
    private static ValueCardinality ofComputedValue(Expression expression) {
        if (isScalarLiteral(expression) || isMvReducer(expression)) {
            return SINGLE;
        }
        // Any other function is unknown. Even a function of a single value can return a list, for example SPLIT.
        // TODO add more functions that are known to return single values.
        return MAYBE_MULTI;
    }

    /**
     * Whether {@code expression} is a function that reduces any input to at most one value.
     */
    private static boolean isMvReducer(Expression expression) {
        return expression instanceof MvMin
            || expression instanceof MvMax
            || expression instanceof MvFirst
            || expression instanceof MvLast
            || expression instanceof MvCount;
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
