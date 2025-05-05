/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

public final class PushDownJoinPastProject extends OptimizerRules.OptimizerRule<Join> {
    @Override
    protected LogicalPlan rule(Join join) {
        if (join.left() instanceof Project projectChild
            && JoinTypes.LEFT.equals(join.config().type())
            && join.right() instanceof EsRelation lookupIndex
            && lookupIndex.indexMode() == IndexMode.LOOKUP) {
            // TODO: refactor the relevant part of pushGeneratingPlanPastProjectAndOrderBy and re-use it here.
            return join;
        }

        return join;

    }
}
