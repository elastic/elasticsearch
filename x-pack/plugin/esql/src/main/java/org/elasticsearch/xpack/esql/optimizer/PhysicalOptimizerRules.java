/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;
import org.elasticsearch.xpack.esql.optimizer.rules.OptimizerRules.TransformDirection;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

public class PhysicalOptimizerRules {

    public abstract static class ParameterizedOptimizerRule<SubPlan extends PhysicalPlan, P> extends ParameterizedRule<
        SubPlan,
        PhysicalPlan,
        P> {

        private final TransformDirection direction;

        public ParameterizedOptimizerRule() {
            this(TransformDirection.DOWN);
        }

        protected ParameterizedOptimizerRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan, P context) {
            return direction == TransformDirection.DOWN
                ? plan.transformDown(typeToken(), t -> rule(t, context))
                : plan.transformUp(typeToken(), t -> rule(t, context));
        }

        protected abstract PhysicalPlan rule(SubPlan plan, P context);
    }

    public abstract static class OptimizerRule<SubPlan extends PhysicalPlan> extends Rule<SubPlan, PhysicalPlan> {

        private final TransformDirection direction;

        public OptimizerRule() {
            this(TransformDirection.DOWN);
        }

        protected OptimizerRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return direction == TransformDirection.DOWN
                ? plan.transformDown(typeToken(), this::rule)
                : plan.transformUp(typeToken(), this::rule);
        }

        protected abstract PhysicalPlan rule(SubPlan plan);
    }

    public abstract static class OptimizerExpressionRule<E extends Expression> extends Rule<PhysicalPlan, PhysicalPlan> {

        private final TransformDirection direction;
        // overriding type token which returns the correct class but does an uncheck cast to LogicalPlan due to its generic bound
        // a proper solution is to wrap the Expression rule into a Plan rule but that would affect the rule declaration
        // so instead this is hacked here
        private final Class<E> expressionTypeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        public OptimizerExpressionRule(TransformDirection direction) {
            this.direction = direction;
        }

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return direction == TransformDirection.DOWN
                ? plan.transformExpressionsDown(expressionTypeToken, this::rule)
                : plan.transformExpressionsUp(expressionTypeToken, this::rule);
        }

        protected PhysicalPlan rule(PhysicalPlan plan) {
            return plan;
        }

        protected abstract Expression rule(E e);

        public Class<E> expressionToken() {
            return expressionTypeToken;
        }
    }
}
