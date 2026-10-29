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
     * Whether {@code expression} is single-valued at {@code plan}. The {@code plan} is the node, or stage, of the pipeline that the
     * expression is evaluated at: it stands for the pipeline from the source up to that node, and the columns that the expression reads
     * must be columns that this node outputs. For an expression that belongs to a node, such as a grouping of a {@code STATS}, that is the
     * child of the node, since the node reads the columns that its child outputs.
     */
    static ValueCardinality of(Expression expression, LogicalPlan plan) {
        return ofExpression(expression, plan, 0);
    }

    /**
     * Whether {@code expression}, which is written in {@code node}, is single-valued. This is the usual question for a rule that holds
     * a node and one of its expressions, such as a grouping of a {@code STATS}. The expression reads the columns that the node reads,
     * so this picks the plan to pass to {@link #of} from the type of the node, and the caller does not have to know it. For a node that
     * is not listed, the answer is {@link #MAYBE_MULTI}.
     */
    static ValueCardinality ofWrittenIn(Expression expression, LogicalPlan node) {
        if (node instanceof Eval || node instanceof Row) {
            // These nodes can read their own earlier fields, so the expression is evaluated over the node itself.
            // FROM test | EVAL a = MV_MAX(languages), k = a the field k = a reads a, which this EVAL defines
            return of(expression, node);
        }
        if (node instanceof Aggregate
            || node instanceof Filter
            || node instanceof OrderBy
            || node instanceof Project
            || node instanceof Limit
            || node instanceof TopN) {
            // These nodes read the columns that their child outputs.
            // FROM test | EVAL a = MV_MIN(languages) | STATS BY a + 1 the grouping a + 1 reads a, from the EVAL below
            return of(expression, node.children().get(0));
        }
        return MAYBE_MULTI;
    }

    /**
     * Follows {@code attribute} down {@code plan}, one node at a time, to the node that produced it. The question at each node is
     * "is this attribute single-valued as it comes out of {@code plan}?". Each case below is one kind of node, with a query that reaches
     * it. In the examples, the key of the last {@code STATS} is the attribute that is asked about.
     */
    private static ValueCardinality ofAttribute(Attribute attribute, LogicalPlan plan, int depth) {
        if (depth > MAX_DEPTH) {
            return MAYBE_MULTI;
        }
        // Each case either finds the producer of the attribute or hands the question to the child. Plan nodes that are not
        // listed may rewrite columns, so they end the search.
        if (plan instanceof Eval eval) {
            // EVAL: the attribute is either a field that this EVAL defines, or a column that passes through it.
            // FROM test | EVAL a = MV_MAX(languages), k = a | STATS BY k
            // For k, this EVAL defines k = a, so we go on and ask about the expression "a". For a column that this EVAL does not
            // define, such as languages in "EVAL a = 1 | STATS BY languages", we ask the child.
            Alias definition = definitionOf(attribute, eval.fields());
            // A field may read the fields before it in the same Eval (k = a above), so its definition is resolved against this Eval, not
            // its child.
            return definition != null ? ofExpression(definition.child(), eval, depth + 1) : ofAttribute(attribute, eval.child(), depth + 1);
        }
        if (plan instanceof Row row) {
            // ROW: the attribute is a field of the row. Nothing is below a ROW, so a column that it does not define is unknown.
            // ROW x = 1 | STATS BY x -> the definition is the constant 1, so SINGLE
            // ROW x = [1, 2] | STATS BY x -> the definition is a multi-valued literal, so unknown
            Alias definition = definitionOf(attribute, row.fields());
            return definition != null ? ofExpression(definition.child(), plan, depth + 1) : MAYBE_MULTI;
        }
        if (plan instanceof Project project) {
            // KEEP, DROP and RENAME produce a Project, a list of kept columns and renamed columns.
            // FROM test | EVAL a = MV_MAX(languages) | RENAME a AS k | STATS BY k
            // A rename is an Alias with a new id (k = a). Its source is a column of the child, so it is resolved against the child. The
            // projections do not read each other (RENAME a AS b, b AS a swaps two columns), so never against this node.
            // A kept column keeps its id, so the child is asked about it.
            for (NamedExpression projection : project.projections()) {
                if (projection.id().equals(attribute.id())) {
                    return projection instanceof Alias alias
                        ? ofExpression(alias.child(), project.child(), depth + 1)
                        : ofAttribute(attribute, project.child(), depth + 1);
                }
            }
            // The attribute is not an output of this node.
            return MAYBE_MULTI;
        }
        if (plan instanceof MvExpand mvExpand) {
            // MV_EXPAND puts each value of a multi-valued column on a row of its own, so the expanded column holds one value per row.
            // FROM test | MV_EXPAND languages | STATS BY languages -> SINGLE
            // Any other column is a column of the child, so the child is asked about it:
            // FROM test | MV_EXPAND languages | STATS BY salary
            return mvExpand.expanded().id().equals(attribute.id()) ? SINGLE : ofAttribute(attribute, mvExpand.child(), depth + 1);
        }
        if (plan instanceof Aggregate aggregate) {
            // An earlier STATS: the attribute is one of its output columns. See ofAggregateOutput.
            // FROM test | STATS c = COUNT(*) BY first_name | STATS n = COUNT(*) BY c
            return ofAggregateOutput(attribute, aggregate);
        }
        if (plan instanceof EsRelation) {
            // FROM: the search reached the source. A field is single-valued only if the mapping of every resolved index says so. A field
            // that may be read from _source or converted is not a plain mapped field, so its EsField never carries the flag.
            // FROM test | STATS BY languages
            return attribute instanceof FieldAttribute field && field.field().isStaticallySingleValued() ? SINGLE : MAYBE_MULTI;
        }
        if (plan instanceof Filter || plan instanceof Limit || plan instanceof OrderBy || plan instanceof TopN) {
            // WHERE, LIMIT and SORT keep, drop or reorder rows and do not change columns, so the child is asked. The optimizer turns
            // SORT followed by LIMIT into one TopN, which does the same.
            // FROM test | EVAL k = MV_MIN(languages) | WHERE k > 1 | SORT k | LIMIT 5 | STATS BY k
            return ofAttribute(attribute, plan.children().get(0), depth + 1);
        }
        // Any other node, such as LOOKUP JOIN or GROK, may rewrite columns, so nothing is known.
        return MAYBE_MULTI;
    }

    /**
     * The same question for the right-hand side of a definition, such as the {@code MV_MAX(languages)} in
     * {@code EVAL a = MV_MAX(languages)}. {@code plan} is where to look for the columns that the expression reads.
     */
    private static ValueCardinality ofExpression(Expression expression, LogicalPlan plan, int depth) {
        if (depth > MAX_DEPTH) {
            return MAYBE_MULTI;
        }
        if (isScalarFoldable(expression)) {
            // A constant: it folds to null or to one value, but not to a list.
            // FROM test | EVAL k = 1 | STATS BY k -> SINGLE
            // FROM test | EVAL k = [1, 2] | STATS BY k -> a multi-valued literal folds to a list, so unknown
            return SINGLE;
        }
        if (expression instanceof Alias alias) {
            // A definition that is itself an alias: look at what it names.
            return ofExpression(alias.child(), plan, depth + 1);
        }
        if (expression instanceof Attribute attribute) {
            // A copy of another column: ask about that column.
            // FROM test | EVAL a = MV_MAX(languages) | EVAL k = a | STATS BY k
            return ofAttribute(attribute, plan, depth + 1);
        }
        if (expression instanceof MvMin
            || expression instanceof MvMax
            || expression instanceof MvFirst
            || expression instanceof MvLast
            || expression instanceof MvCount) {
            // These functions turn any input, even a list, into at most one value.
            // FROM test | EVAL k = MV_MAX(languages) | STATS BY k
            return SINGLE;
        }
        // Any other function is unknown. A function of a single value is not assumed single: SPLIT returns a list from one input.
        // FROM test | EVAL k = languages + 1 | STATS BY k
        // FROM test | EVAL k = SPLIT(first_name, "a") | STATS BY k
        return MAYBE_MULTI;
    }

    /**
     * The same question for a column that an earlier {@code STATS} outputs. The outputs are its aggregate functions and its grouping
     * keys.
     */
    private static ValueCardinality ofAggregateOutput(Attribute attribute, Aggregate aggregate) {
        for (NamedExpression output : aggregate.aggregates()) {
            if (output.id().equals(attribute.id()) == false) {
                // skip columns that aren't the attribute we are looking for
                continue;
            }
            Expression unwrapped = Alias.unwrap(output);
            if (unwrapped instanceof Count) {
                // COUNT returns one number per group.
                // FROM test | STATS c = COUNT(*) BY first_name | STATS n = COUNT(*) BY c
                return SINGLE;
            }
            // Other aggregate functions can return a list (VALUES, TOP, ...), so only the key itself is proven. A grouping
            // key holds one value per group even when the input column was a list, because the aggregation unrolls the list.
            // FROM test | STATS c = COUNT(*) BY languages | STATS n = COUNT(*) BY languages
            // -> languages is a key: SINGLE
            // FROM test | STATS v = VALUES(languages) BY first_name | STATS n = COUNT(*) BY v
            // -> VALUES returns a list: unknown
            return isGroupKey(aggregate, output) ? SINGLE : MAYBE_MULTI;
        }
        // The attribute is not an output of this STATS.
        return MAYBE_MULTI;
    }

    /**
     * Whether {@code output} is one of the grouping keys of {@code aggregate}, rather than an aggregate function.
     */
    private static boolean isGroupKey(Aggregate aggregate, NamedExpression output) {
        Expression unwrappedOutput = Alias.unwrap(output);
        for (Expression grouping : aggregate.groupings()) {
            // BY x + 1: the output is an alias of the same expression as the grouping.
            // BY languages: the grouping is the column languages, and the output has the same id.
            if (unwrappedOutput.semanticEquals(Alias.unwrap(grouping))
                || (grouping instanceof NamedExpression named && named.id().equals(output.id()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * The field of an {@code EVAL} or {@code ROW} that defines {@code attribute}, found by id, or {@code null} if it defines none.
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
