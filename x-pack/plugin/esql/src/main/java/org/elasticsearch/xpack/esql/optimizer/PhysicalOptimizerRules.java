/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.rule.Rule;

public class PhysicalOptimizerRules {

    public interface ParameterizedOptimizerRule<SubPlan extends PhysicalPlan, P> extends ParameterizedRule<SubPlan, PhysicalPlan, P> {
        abstract class Sync<SubPlan extends PhysicalPlan, P> extends ParameterizedRule.Sync<SubPlan, PhysicalPlan, P>
            implements
                ParameterizedOptimizerRule<SubPlan, P> {

            private final TransformDirection direction;

            public Sync() {
                this(OptimizerRules.TransformDirection.DOWN);
            }

            protected Sync(TransformDirection direction) {
                this.direction = direction;
            }

            @Override
            public final PhysicalPlan apply(PhysicalPlan plan, P context) {
                return direction == OptimizerRules.TransformDirection.DOWN
                    ? plan.transformDown(typeToken(), t -> rule(t, context))
                    : plan.transformUp(typeToken(), t -> rule(t, context));
            }

            protected abstract PhysicalPlan rule(SubPlan plan, P context);
        }
    }

    public interface OptimizerRule<SubPlan extends PhysicalPlan> extends Rule<SubPlan, PhysicalPlan> {
        abstract class Sync<SubPlan extends PhysicalPlan> extends Rule.Sync<SubPlan, PhysicalPlan> implements OptimizerRule<SubPlan> {

            private final TransformDirection direction;

            public Sync() {
                this(OptimizerRules.TransformDirection.DOWN);
            }

            protected Sync(TransformDirection direction) {
                this.direction = direction;
            }

            @Override
            public final PhysicalPlan apply(PhysicalPlan plan) {
                return direction == OptimizerRules.TransformDirection.DOWN
                    ? plan.transformDown(typeToken(), this::rule)
                    : plan.transformUp(typeToken(), this::rule);
            }

            protected abstract PhysicalPlan rule(SubPlan plan);
        }
    }
}
