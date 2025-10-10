/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;

public final class PushDownMvExpandPastProject extends OptimizerRules.OptimizerRule<MvExpand> {
    @Override
    protected LogicalPlan rule(MvExpand mvExpand) {
        if (mvExpand.child() instanceof Project pj
            && pj.projections()
                .stream()
                .anyMatch(
                    p -> p instanceof Alias alias
                        && (alias.toAttribute().equals(mvExpand.target().toAttribute())
                            || alias.references().contains(mvExpand.target().toAttribute()))
                ) == false) {
            Project project = PushDownUtils.pushDownPastProject(mvExpand);
            return PushDownUtils.resolveRenamesFromMap(project, AttributeMap.of(mvExpand.target().toAttribute(), mvExpand.expanded()));
        }
        return mvExpand;
    }
}
