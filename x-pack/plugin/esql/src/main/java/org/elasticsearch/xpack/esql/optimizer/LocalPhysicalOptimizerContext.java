/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.datasources.FilterPushdownRegistry;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public record LocalPhysicalOptimizerContext(
    PlannerSettings plannerSettings,
    EsqlFlags flags,
    Configuration configuration,
    FoldContext foldCtx,
    SearchStats searchStats,
    FilterPushdownRegistry filterPushdownRegistry
) {
    /**
     * Convenience constructor without filter pushdown registry (for backward compatibility).
     */
    public LocalPhysicalOptimizerContext(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        SearchStats searchStats
    ) {
        this(plannerSettings, flags, configuration, foldCtx, searchStats, FilterPushdownRegistry.empty());
    }
}
