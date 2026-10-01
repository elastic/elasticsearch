/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads an empty string as a null where it meets a field that holds none, because an empty string given at index
 * time was read as a null there.
 *
 * <p>An empty string reaches a query in one of two ways, and each is answered here.
 *
 * <p>Written as a question — {@code field == ""} — it asks whether the field holds a value, so it becomes
 * {@link IsNull}, and its negation {@link IsNotNull}. Comparing against a null would answer null instead of
 * selecting the documents meant.
 *
 * <p>Written as a value — {@code CASE(condition, field, "")}, {@code COALESCE(field, "")} — it stands for the
 * absence the field expresses as a null, so the literal itself becomes null and the two spellings fall into one
 * group rather than two. What marks it as that rather than an ordinary empty string is the field beside it: a
 * literal with no such field among its siblings is left alone.
 *
 * <p>The rewrite is made here rather than on the coordinator because only a data node knows how the indices it
 * holds read an empty string. Rewriting before the filter is pushed also keeps the two answers the same: the
 * pushed-down query and the expression evaluated in the compute engine are built from one expression.
 */
public class ReplaceEmptyStringWithNull extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        return plan.transformExpressionsDown(Expression.class, e -> rewrite(e, context));
    }

    /**
     * Visited top down, so a negated comparison is answered before the comparison inside it and becomes one
     * {@link IsNotNull} rather than a negated {@link IsNull}.
     */
    private static Expression rewrite(Expression expression, LocalLogicalOptimizerContext context) {
        if (expression instanceof Not not && not.field() instanceof Equals equals) {
            final FieldAttribute field = comparedToAnEmptyString(equals.left(), equals.right(), context);
            return field == null ? expression : new IsNotNull(not.source(), field);
        }
        if (expression instanceof NotEquals notEquals) {
            final FieldAttribute field = comparedToAnEmptyString(notEquals.left(), notEquals.right(), context);
            return field == null ? expression : new IsNotNull(notEquals.source(), field);
        }
        if (expression instanceof Equals equals) {
            final FieldAttribute field = comparedToAnEmptyString(equals.left(), equals.right(), context);
            return field == null ? expression : new IsNull(equals.source(), field);
        }
        // A comparison asks a question about the field, which the cases above answer; substituting a null into
        // one would answer null. `In` is left as it stands for the same reason.
        if (expression instanceof EsqlBinaryComparison || expression instanceof In) {
            return expression;
        }
        return substituteNullBeside(expression, context);
    }

    /** The field a comparison against an empty string is asking about, or null for an ordinary comparison. */
    private static FieldAttribute comparedToAnEmptyString(Expression left, Expression right, LocalLogicalOptimizerContext context) {
        final FieldAttribute field = left instanceof FieldAttribute f ? f : right instanceof FieldAttribute f ? f : null;
        final Expression other = left instanceof FieldAttribute ? right : left;
        if (field == null || isEmptyString(other) == false || holdsNoEmptyString(field, context) == false) {
            return null;
        }
        return field;
    }

    /**
     * The expression with every empty string among its children read as a null, when one of the others is a field
     * that holds no empty string. Where several fields are involved, one of them saying so is enough: they are
     * alternatives to each other, so the empty string stands for what any of them would express as a null.
     */
    private static Expression substituteNullBeside(Expression expression, LocalLogicalOptimizerContext context) {
        boolean besideSuchAField = false;
        boolean holdsAnEmptyString = false;
        for (Expression child : expression.children()) {
            besideSuchAField |= child instanceof FieldAttribute field && holdsNoEmptyString(field, context);
            holdsAnEmptyString |= isEmptyString(child);
        }
        if (besideSuchAField == false || holdsAnEmptyString == false) {
            return expression;
        }
        final List<Expression> children = new ArrayList<>(expression.children().size());
        for (Expression child : expression.children()) {
            children.add(isEmptyString(child) ? new Literal(child.source(), null, child.dataType()) : child);
        }
        return expression.replaceChildren(children);
    }

    private static boolean holdsNoEmptyString(FieldAttribute field, LocalLogicalOptimizerContext context) {
        return context.searchStats().emptyStringReadsAsNull(field.fieldName());
    }

    private static boolean isEmptyString(Expression expression) {
        return expression instanceof Literal literal && literal.value() instanceof BytesRef bytes && bytes.length == 0;
    }
}
