/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

public final class PushDownMvExpandPastProject extends OptimizerRules.OptimizerRule<MvExpand> {
    @Override
    protected LogicalPlan rule(MvExpand mvExpand) {
        if (mvExpand.child() instanceof Project pj) {
            List<NamedExpression> projections = new ArrayList<>(pj.projections());

            // Find if the target is aliased in the project and create an alias with temporary names for it.
            for (int i = 0; i < projections.size(); i++) {
                NamedExpression projection = projections.get(i);
                if (projection instanceof Alias alias && alias.toAttribute().equals(mvExpand.target().toAttribute())) {
                    Alias aliasAlias = new Alias(
                        alias.source(),
                        TemporaryNameUtils.temporaryName(alias.child(), alias.toAttribute(), 0),
                        alias.child()
                    );
                    projections.set(i, mvExpand.expanded());
                    pj = new Project(pj.source(), new Eval(aliasAlias.source(), pj.child(), List.of(aliasAlias)), projections);
                    mvExpand = new MvExpand(mvExpand.source(), pj, aliasAlias.toAttribute(), mvExpand.expanded());
                    break;
                }
            }

            Project project = PushDownUtils.pushDownPastProject(mvExpand);
            return PushDownUtils.resolveRenamesFromMap(project, AttributeMap.of(mvExpand.target().toAttribute(), mvExpand.expanded()));
        }
        return mvExpand;
    }
}
