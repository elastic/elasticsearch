/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * This class is part of the planner.  Acts somewhat like a linker, to find the indices and enrich policies referenced by the query.
 */
public class PreAnalyzer {

    public static class PreAnalysis {
        public static final PreAnalysis EMPTY = new PreAnalysis(null, emptyList(), emptyList(), emptyList(), emptyList());

        public final IndexMode indexMode;
        public final List<IndexPattern> indices;
        public final List<Enrich> enriches;
        public final List<InferencePlan<?>> inferencePlans;
        public final List<IndexPattern> lookupIndices;

        public PreAnalysis(
            IndexMode indexMode,
            List<IndexPattern> indices,
            List<Enrich> enriches,
            List<InferencePlan<?>> inferencePlans,
            List<IndexPattern> lookupIndices
        ) {
            this.indexMode = indexMode;
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
        Set<IndexPattern> indices = new HashSet<>();

        List<Enrich> unresolvedEnriches = new ArrayList<>();
        List<IndexPattern> lookupIndices = new ArrayList<>();
        List<InferencePlan<?>> unresolvedInferencePlans = new ArrayList<>();
        Holder<IndexMode> indexMode = new Holder<>();
        plan.forEachUp(UnresolvedRelation.class, p -> {
            if (p.indexMode() == IndexMode.LOOKUP) {
                lookupIndices.add(p.indexPattern());
            } else if (indexMode.get() == null || indexMode.get() == p.indexMode()) {
                indexMode.set(p.indexMode());
                indices.add(p.indexPattern());
            } else {
                throw new IllegalStateException("index mode is already set");
            }
        });

        plan.forEachUp(Enrich.class, unresolvedEnriches::add);
        plan.forEachUp(InferencePlan.class, unresolvedInferencePlans::add);

        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);

        return new PreAnalysis(indexMode.get(), indices.stream().toList(), unresolvedEnriches, unresolvedInferencePlans, lookupIndices);
    }
}
