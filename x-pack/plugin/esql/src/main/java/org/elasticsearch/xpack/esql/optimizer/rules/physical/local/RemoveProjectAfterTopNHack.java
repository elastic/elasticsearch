/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

// FIXME(gal, NOCOMMIT) Document
// FIXME(gal, NOCOMMIT) This should only run if there is a node-level reduction!
public class RemoveProjectAfterTopNHack extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    ProjectExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(ProjectExec plan, LocalPhysicalOptimizerContext context) {
        // return plan;
        if (plan.child() instanceof FieldExtractExec fieldExtract) {
            if (fieldExtract.child() instanceof TopNExec topN) {
                return topN;
            }
        }
        return plan;
    }
}
