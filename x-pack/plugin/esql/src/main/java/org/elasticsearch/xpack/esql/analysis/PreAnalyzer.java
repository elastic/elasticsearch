/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
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

    public record PreAnalysis(
        IndexMode indexMode,
        IndexPattern indexPattern,
        List<Enrich> enriches,
        List<IndexPattern> lookupIndices,
        boolean supportsAggregateMetricDouble,
        boolean supportsDenseVector
    ) {
        public static final PreAnalysis EMPTY = new PreAnalysis(null, null, List.of(), List.of(), false, false);
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

        List<Enrich> unresolvedEnriches = new ArrayList<>();
        plan.forEachUp(Enrich.class, unresolvedEnriches::add);

        /*
         * Enable aggregate_metric_double and dense_vector when we see certain function
         * or the TS command. This allows us to release these when not all nodes understand
         * these types. These functions are only supported on newer nodes, so we use them
         * as a signal that the query is only for nodes that support these types.
         *
         * This work around is temporary until we flow the minimum transport version
         * back through a cross cluster search field caps call.
         */
        Holder<Boolean> supportsAggregateMetricDouble = new Holder<>(false);
        Holder<Boolean> supportsDenseVector = new Holder<>(false);
        plan.forEachDown(p -> p.forEachExpression(UnresolvedFunction.class, fn -> {
            if (fn.name().equalsIgnoreCase("knn")
                || fn.name().equalsIgnoreCase("to_dense_vector")
                || fn.name().equalsIgnoreCase("v_cosine")
                || fn.name().equalsIgnoreCase("v_hamming")
                || fn.name().equalsIgnoreCase("v_l1_norm")
                || fn.name().equalsIgnoreCase("v_l2_norm")
                || fn.name().equalsIgnoreCase("v_dot_product")
                || fn.name().equalsIgnoreCase("v_magnitude")) {
                supportsDenseVector.set(true);
            }
            if (fn.name().equalsIgnoreCase("to_aggregate_metric_double")) {
                supportsAggregateMetricDouble.set(true);
            }
        }));

        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);

        return new PreAnalysis(
            indexMode.get(),
            index.get(),
            unresolvedEnriches,
            lookupIndices,
            indexMode.get() == IndexMode.TIME_SERIES || supportsAggregateMetricDouble.get(),
            supportsDenseVector.get()
        );
    }
}
