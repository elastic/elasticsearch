/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsqlUnresolvedRelation;
import org.elasticsearch.xpack.ql.analyzer.TableInfo;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class PreAnalyzer {

    public static class PreAnalysis {
        public static final PreAnalysis EMPTY = new PreAnalysis(emptyList(), emptyList());

        public final List<TableInfo> indices;
        public final List<String> policyNames;

        public PreAnalysis(List<TableInfo> indices, List<String> policyNames) {
            this.indices = indices;
            this.policyNames = policyNames;
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
        List<String> policyNames = new ArrayList<>();

        plan.forEachUp(EsqlUnresolvedRelation.class, p -> indices.add(new TableInfo(p.table(), p.frozen())));
        plan.forEachUp(Enrich.class, p -> policyNames.add((String) p.policyName().fold()));

        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);

        return new PreAnalysis(indices, policyNames);
    }
}
