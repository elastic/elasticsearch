/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

/**
 * Transforms a physical plan for EXPLAIN mode by replacing data source nodes
 * with empty local sources. This makes execution essentially free while
 * preserving the plan structure for profiling.
 */
public final class ExplainPlanTransformer {

    private ExplainPlanTransformer() {}

    /**
     * Transforms the plan by replacing ES data source nodes with empty LocalSourceExec.
     * This preserves the plan structure but makes execution cheap since no data is read.
     *
     * @param plan the original physical plan
     * @return a transformed plan with empty data sources
     */
    public static PhysicalPlan replaceDataSourcesWithEmpty(PhysicalPlan plan) {
        return plan.transformUp(PhysicalPlan.class, p -> {
            if (p instanceof EsQueryExec esQuery) {
                // Replace EsQueryExec with empty LocalSourceExec preserving output schema
                return new LocalSourceExec(esQuery.source(), esQuery.output(), EmptyLocalSupplier.EMPTY);
            }
            if (p instanceof EsSourceExec esSource) {
                // Replace EsSourceExec with empty LocalSourceExec preserving output schema
                return new LocalSourceExec(esSource.source(), esSource.output(), EmptyLocalSupplier.EMPTY);
            }
            if (p instanceof EsStatsQueryExec esStats) {
                // Replace EsStatsQueryExec with empty LocalSourceExec preserving output schema
                return new LocalSourceExec(esStats.source(), esStats.output(), EmptyLocalSupplier.EMPTY);
            }
            return p;
        });
    }
}
