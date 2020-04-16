/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

// Since the pre-analyzer only inspect (and does NOT transform) the tree
// it is not built as a rule executor.
// Further more it applies 'the rules' only once and needs to return some
// state back.
public class PreAnalyzer {

    public static class PreAnalysis {
        public static final PreAnalysis EMPTY = new PreAnalysis(emptyList());

        public final List<TableInfo> indices;

        PreAnalysis(List<TableInfo> indices) {
            this.indices = indices;
        }
    }

    public PreAnalysis preAnalyze(LogicalPlan plan) {
        if (plan.analyzed()) {
            return PreAnalysis.EMPTY;
        }

        return doPreAnalyze(plan);
    }

    private PreAnalysis doPreAnalyze(LogicalPlan plan) {
        List<TableInfo> indices = new ArrayList<>();
        
        plan.forEachUp(p -> indices.add(new TableInfo(p.table(), p.frozen())), UnresolvedRelation.class);
        
        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);
        
        return new PreAnalysis(indices);
    }
}