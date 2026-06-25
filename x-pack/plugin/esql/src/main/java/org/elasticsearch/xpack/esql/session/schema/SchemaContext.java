/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Per-query state threaded into the schema providers so the index-resolution orchestration can be carried out
 * away from the session. Bundles the request-scoped inputs the providers read — the execution info they mutate
 * as clusters resolve, the configuration (project routing, partial-results), the pre-analysis (index patterns,
 * lookup indices, feature flags), the optional request filter, and whether unmapped field indices are tracked.
 *
 * <p>The view-resolution inputs ({@code projectRouting}, {@code viewParser}) are carried here too so a view name
 * can be resolved through the umbrella's {@code resolveSchema} dispatch. They are nullable because the index- and
 * dataset-resolution call sites do not run view resolution and leave them unset; only the view-schema path reads
 * them. A {@code (query, viewName) -> LogicalPlan} parser is what {@code ViewResolver} needs to expand a stored
 * view query into its body plan.
 */
public record SchemaContext(
    EsqlExecutionInfo executionInfo,
    Configuration configuration,
    PreAnalyzer.PreAnalysis preAnalysis,
    QueryBuilder requestFilter,
    boolean trackUnmappedFieldIndices,
    Set<String> fieldNames,
    TransportVersion minimumVersion,
    @Nullable String projectRouting,
    @Nullable BiFunction<String, String, LogicalPlan> viewParser
) {
    /**
     * Build a context for the index/dataset resolution call sites, which do not drive view resolution and so leave
     * the view-parsing inputs unset. The view-schema path uses the canonical constructor with those inputs supplied.
     */
    public SchemaContext(
        EsqlExecutionInfo executionInfo,
        Configuration configuration,
        PreAnalyzer.PreAnalysis preAnalysis,
        QueryBuilder requestFilter,
        boolean trackUnmappedFieldIndices,
        Set<String> fieldNames,
        TransportVersion minimumVersion
    ) {
        this(executionInfo, configuration, preAnalysis, requestFilter, trackUnmappedFieldIndices, fieldNames, minimumVersion, null, null);
    }
}
