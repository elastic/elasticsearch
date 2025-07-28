/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
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

    public abstract static class OptimizerExpressionRule<E extends Expression> extends ParameterizedRule<
        LogicalPlan,
        LogicalPlan,
        LogicalOptimizerContext> {

        private final TransformDirection direction;
        // overriding type token which returns the correct class but does an uncheck cast to LogicalPlan due to its generic bound
        // a proper solution is to wrap the Expression rule into a Plan rule but that would affect the rule declaration
        // so instead this is hacked here
        private final Class<E> expressionTypeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        public OptimizerExpressionRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
            return direction == TransformDirection.DOWN
                ? plan.transformExpressionsDown(this::shouldVisit, expressionTypeToken, e -> rule(e, ctx))
                : plan.transformExpressionsUp(this::shouldVisit, expressionTypeToken, e -> rule(e, ctx));
        }

        protected abstract Expression rule(E e, LogicalOptimizerContext ctx);

        /**
         * Defines if a node should be visited or not.
         * Allows to skip nodes that are not applicable for the rule even if they contain expressions.
         * By default that skips FROM, LIMIT, PROJECT, KEEP and DROP but this list could be extended or replaced in subclasses.
         */
        protected boolean shouldVisit(Node<?> node) {
            return switch (node) {
                case EsRelation relation -> false;
                case Project project -> false;// this covers project, keep and drop
                case Limit limit -> false;
                default -> true;
            };
        }

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

        @Override
        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return direction == TransformDirection.DOWN
                ? plan.transformDown(typeToken(), t -> rule(t, context))
                : plan.transformUp(typeToken(), t -> rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);
    }
}
