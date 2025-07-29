/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * This class is part of the planner.  Acts somewhat like a linker, to find the indices and enrich policies referenced by the query.
 */
public class PreAnalyzer {

    public static class PreAnalysis {
        public static final PreAnalysis EMPTY = new PreAnalysis(emptyList(), emptyList(), emptyList(), emptyList());

        public final List<TableInfo> indices;
        public final List<Enrich> enriches;
        public final List<InferencePlan<?>> inferencePlans;
        public final List<TableInfo> lookupIndices;

        public PreAnalysis(
            List<TableInfo> indices,
            List<Enrich> enriches,
            List<InferencePlan<?>> inferencePlans,
            List<TableInfo> lookupIndices
        ) {
            this.indices = indices;
            this.enriches = enriches;
            this.inferencePlans = inferencePlans;
            this.lookupIndices = lookupIndices;
        }
    }

    public PreAnalysis preAnalyze(LogicalPlan plan) {
        if (plan.analyzed()) {
            return PreAnalysis.EMPTY;
        }

        return doPreAnalyze(plan);
    }

    protected PreAnalysis doPreAnalyze(LogicalPlan plan) {
        List<TableInfo> indices = new ArrayList<>();
        List<Enrich> unresolvedEnriches = new ArrayList<>();
        List<TableInfo> lookupIndices = new ArrayList<>();
        List<InferencePlan<?>> unresolvedInferencePlans = new ArrayList<>();

        plan.forEachUp(UnresolvedRelation.class, p -> {
            List<TableInfo> list = p.indexMode() == IndexMode.LOOKUP ? lookupIndices : indices;
            list.add(new TableInfo(p.indexPattern()));
        });
        plan.forEachUp(Enrich.class, unresolvedEnriches::add);
        plan.forEachUp(InferencePlan.class, unresolvedInferencePlans::add);

        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);

        return new PreAnalysis(indices, unresolvedEnriches, unresolvedInferencePlans, lookupIndices);
    }
}
