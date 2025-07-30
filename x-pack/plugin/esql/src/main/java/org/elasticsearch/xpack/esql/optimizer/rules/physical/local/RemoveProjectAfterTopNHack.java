/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

// FIXME(gal, NOCOMMIT) Document
public class RemoveProjectAfterTopNHack extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    ProjectExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(ProjectExec project, LocalPhysicalOptimizerContext context) {
        if (project.child() instanceof TopNExec topN) {
            Holder<Boolean> hasMultiIndex = new Holder<>(false);
            topN.transformDown(EsQueryExec.class, qe -> {
                if (qe.indexNameWithModes().size() > 1) {
                    hasMultiIndex.set(true);
                }
                return qe;
            });
            if (hasMultiIndex.get() == false) {
                // TODO This isn't necessarily optimal, but it's a very simple way to ensure that the data node's output is equivalent to
                // what
                // the coordinator expects. There are cases where this creates redundancy though, e.g., if a filter is pushed down to
                // source,
                // this project will still cause it to be fetched. We should fix this in the future.
                return new ProjectExec(project.source(), topN, new InsertFieldExtraction().rule(topN, context).output());
            }
        }
        return project;
    }
}
