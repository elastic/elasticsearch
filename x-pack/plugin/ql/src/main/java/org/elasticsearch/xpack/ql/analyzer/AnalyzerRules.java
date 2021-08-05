/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.analyzer;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.Rule;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;

public final class AnalyzerRules {

    public static class AddMissingEqualsToBoolField extends AnalyzerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            if (filter.resolved() == false) {
                return filter;
            }
            // check the condition itself
            Expression condition = replaceRawBoolFieldWithEquals(filter.condition());
            // otherwise look for binary logic
            if (condition == filter.condition()) {
                condition = condition.transformUp(BinaryLogic.class, b ->
                    b.replaceChildren(asList(replaceRawBoolFieldWithEquals(b.left()), replaceRawBoolFieldWithEquals(b.right())))
                );
            }

            if (condition != filter.condition()) {
                filter = filter.with(condition);
            }
            return filter;
        }

        private Expression replaceRawBoolFieldWithEquals(Expression e) {
            if (e instanceof FieldAttribute && e.dataType() == BOOLEAN) {
                e = new Equals(e.source(), e, Literal.of(e, Boolean.TRUE));
            }
            return e;
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }


    public abstract static class AnalyzerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(typeToken(), t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t));
        }

        @Override
        protected abstract LogicalPlan rule(SubPlan plan);

        protected boolean skipResolved() {
            return true;
        }
    }

    public abstract static class BaseAnalyzerRule extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            return doRule(plan);
        }

        protected abstract LogicalPlan doRule(LogicalPlan plan);
    }
}
