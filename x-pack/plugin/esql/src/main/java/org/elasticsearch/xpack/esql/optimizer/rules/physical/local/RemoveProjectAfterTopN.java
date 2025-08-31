/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

/**
 * Removes a {@link ProjectExec} that follows a {@link TopNExec}.
 *
 * This step is needed since said project will activate {@link InsertFieldExtraction} later on, whereas we wish to perform these
 * extractions in the node reduce driver, after performing multiple Top N operations, thereby reducing the amount of data we need to
 * read from the index.
 *
 * The basic strategy here is to cut off the operation right after the last top n, and perform all the removed operations on the
 * reduce-side, so all the data-side top n operations "feed into" the reduce-side one.
 */
public class RemoveProjectAfterTopN extends PhysicalOptimizerRules.ParameterizedOptimizerRule<ProjectExec, LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(ProjectExec project, LocalPhysicalOptimizerContext context) {
        if (project.child() instanceof TopNExec topN && isTopNCompatible(topN)) {
            // Technically, we don't *remove* the project, but rather replace it with a new one that has the exact same expressions as the
            // underlying TopN. This is to avoid cases where the top n is pushed down to the source in subsequent rules, and thus its output
            // schema is different from what the reduce-side top n expects as input.

            // TODO This isn't necessarily optimal, but it's a very simple way to ensure that the data node's output is equivalent to
            // what the coordinator expects. There are cases where this creates redundancy though, e.g., if a filter is pushed down to
            // source, this project will still cause it to be fetched. We should fix this in the future.
            return new ProjectExec(project.source(), topN, new InsertFieldExtraction().rule(topN, context).output());
        }
        return project;
    }

    /**
     * We don't support this optimization for multi-index queries at the moment, since the reduce coordinator doesn't actually have access
     * to each individual table's schema, and thus cannot determine the correct output of the data node's physical plan. Similarly, we don't
     * handle enrich or join yet.
     */
    public static boolean isTopNCompatible(PhysicalPlan topN) {
        return topN.anyMatch(plan -> plan instanceof EsQueryExec eqe && eqe.indexNameWithModes().size() > 1) == false
            && topN.anyMatch(plan -> plan instanceof EnrichExec) == false
            && topN.anyMatch(plan -> plan instanceof LookupJoinExec) == false
            && topN.anyMatch(plan -> plan instanceof HashJoinExec) == false;
    }
}
