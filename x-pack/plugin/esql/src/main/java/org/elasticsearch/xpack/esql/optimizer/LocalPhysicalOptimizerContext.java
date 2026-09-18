/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public class LocalPhysicalOptimizerContext {
    private final PlannerSettings plannerSettings;
    private final EsqlFlags flags;
    private final Configuration configuration;
    private final FoldContext foldCtx;
    private final SearchStats searchStats;
    private final ExternalOptimizerContext external;

    public LocalPhysicalOptimizerContext(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        SearchStats searchStats,
        ExternalOptimizerContext external
    ) {
        this.plannerSettings = plannerSettings;
        this.flags = flags;
        this.configuration = configuration;
        this.foldCtx = foldCtx;
        this.searchStats = searchStats;
        this.external = external;
    }

    /**
     * Convenience constructor without external-source state (for backward compatibility and tests
     * that don't exercise external-source rules). External-source-aware rules must treat the
     * resulting context as "no external information" and bail out cleanly.
     */
    public LocalPhysicalOptimizerContext(
        PlannerSettings plannerSettings,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldCtx,
        SearchStats searchStats
    ) {
        this(plannerSettings, flags, configuration, foldCtx, searchStats, ExternalOptimizerContext.NONE);
    }

    public PlannerSettings plannerSettings() {
        return plannerSettings;
    }

    public EsqlFlags flags() {
        return flags;
    }

    public Configuration configuration() {
        return configuration;
    }

    public FoldContext foldCtx() {
        return foldCtx;
    }

    public SearchStats searchStats() {
        return searchStats;
    }

    public ExternalOptimizerContext external() {
        return external;
    }
}
