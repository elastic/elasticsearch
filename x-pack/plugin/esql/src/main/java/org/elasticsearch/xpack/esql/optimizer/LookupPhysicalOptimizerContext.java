/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

/**
 * Context for lookup physical optimization.
 */
public class LookupPhysicalOptimizerContext extends LocalPhysicalOptimizerContext {
    private final AliasFilter aliasFilter;

    public LookupPhysicalOptimizerContext(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        SearchStats searchStats,
        AliasFilter aliasFilter,
        ExternalOptimizerContext external
    ) {
        super(plannerSettings, flags, configuration, foldCtx, searchStats, external);
        this.aliasFilter = aliasFilter;
    }

    public LookupPhysicalOptimizerContext(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        SearchStats searchStats,
        AliasFilter aliasFilter
    ) {
        this(plannerSettings, flags, configuration, foldCtx, searchStats, aliasFilter, ExternalOptimizerContext.NONE);
    }

    public AliasFilter aliasFilter() {
        return aliasFilter;
    }
}
