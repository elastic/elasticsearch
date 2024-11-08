/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.rule.Rule;

public final class OptimizerRules {

    public abstract static class OptimizerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        private final TransformDirection direction;

        public OptimizerRule() {
            this(TransformDirection.DOWN);
        }

        protected OptimizerRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return direction == TransformDirection.DOWN
                ? plan.transformDown(typeToken(), this::rule)
                : plan.transformUp(typeToken(), this::rule);
        }

        protected abstract LogicalPlan rule(SubPlan plan);
    }

    public abstract static class OptimizerExpressionRule<E extends Expression> extends Rule<LogicalPlan, LogicalPlan> {

        private final TransformDirection direction;
        // overriding type token which returns the correct class but does an uncheck cast to LogicalPlan due to its generic bound
        // a proper solution is to wrap the Expression rule into a Plan rule but that would affect the rule declaration
        // so instead this is hacked here
        private final Class<E> expressionTypeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        public OptimizerExpressionRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return direction == TransformDirection.DOWN
                ? plan.transformExpressionsDown(expressionTypeToken, this::rule)
                : plan.transformExpressionsUp(expressionTypeToken, this::rule);
        }

        protected LogicalPlan rule(LogicalPlan plan) {
            return plan;
        }

        protected abstract Expression rule(E e);

        public Class<E> expressionToken() {
            return expressionTypeToken;
        }
    }

    public enum TransformDirection {
        UP,
        DOWN
    }

    public abstract static class ParameterizedOptimizerRule<SubPlan extends LogicalPlan, P> extends ParameterizedRule<
        SubPlan,
        LogicalPlan,
        P> {

        private final TransformDirection direction;

        protected ParameterizedOptimizerRule(TransformDirection direction) {
            this.direction = direction;
        }

        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return direction == TransformDirection.DOWN
                ? plan.transformDown(typeToken(), t -> rule(t, context))
                : plan.transformUp(typeToken(), t -> rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);
    }
}
