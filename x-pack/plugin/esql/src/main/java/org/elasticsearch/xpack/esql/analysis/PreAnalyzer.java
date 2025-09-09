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

import java.util.ArrayList;
import java.util.List;

/**
 * This class is part of the planner.  Acts somewhat like a linker, to find the indices and enrich policies referenced by the query.
 */
public class PreAnalyzer {

    public record PreAnalysis(IndexMode indexMode, IndexPattern index, List<Enrich> enriches, List<IndexPattern> lookupIndices) {
        public static final PreAnalysis EMPTY = new PreAnalysis(null, null, List.of(), List.of());
    }

    public PreAnalysis preAnalyze(LogicalPlan plan) {
        if (plan.analyzed()) {
            return PreAnalysis.EMPTY;
        }

        return doPreAnalyze(plan);
    }

    protected PreAnalysis doPreAnalyze(LogicalPlan plan) {

        Holder<IndexMode> indexMode = new Holder<>();
        Holder<IndexPattern> index = new Holder<>();

        List<Enrich> unresolvedEnriches = new ArrayList<>();
        List<IndexPattern> lookupIndices = new ArrayList<>();

        plan.forEachUp(UnresolvedRelation.class, p -> {
            if (p.indexMode() == IndexMode.LOOKUP) {
                lookupIndices.add(p.indexPattern());
            } else if (indexMode.get() == null || indexMode.get() == p.indexMode()) {
                indexMode.set(p.indexMode());
                index.set(p.indexPattern());
            } else {
                throw new IllegalStateException("index mode is already set");
            }
        });

        plan.forEachUp(Enrich.class, unresolvedEnriches::add);

        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);

        return new PreAnalysis(indexMode.get(), index.get(), unresolvedEnriches, lookupIndices);
    }
}
