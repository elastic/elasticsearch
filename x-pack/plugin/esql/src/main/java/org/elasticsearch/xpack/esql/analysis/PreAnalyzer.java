/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is part of the planner.  Acts somewhat like a linker, to find the indices and enrich policies referenced by the query.
 */
public class PreAnalyzer {

    public record PreAnalysis(
        Map<IndexPattern, IndexMode> indexes,
        List<Enrich> enriches,
        List<IndexPattern> lookupIndices,
        Set<IndexPattern> optionalLinkedIndices,  // CPS only, patterns from local view names that could match remote indices
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        List<String> icebergPaths
    ) {
        public static final PreAnalysis EMPTY = new PreAnalysis(Map.of(), List.of(), List.of(), Set.of(), false, false, false, List.of());
    }

    public PreAnalysis preAnalyze(LogicalPlan plan) {
        if (plan.analyzed()) {
            return PreAnalysis.EMPTY;
        }

        return doPreAnalyze(plan);
    }

    protected PreAnalysis doPreAnalyze(LogicalPlan plan) {
        Map<IndexPattern, IndexMode> indexes = new HashMap<>();
        List<IndexPattern> lookupIndices = new ArrayList<>();
        plan.forEachUp(UnresolvedRelation.class, p -> {
            if (p.indexMode() == IndexMode.LOOKUP) {
                lookupIndices.add(p.indexPattern());
            } else if (indexes.containsKey(p.indexPattern()) == false || indexes.get(p.indexPattern()) == p.indexMode()) {
                indexes.put(p.indexPattern(), p.indexMode());
            } else {
                IndexMode m1 = p.indexMode();
                IndexMode m2 = indexes.get(p.indexPattern());
                throw new IllegalStateException(
                    "index pattern '" + p.indexPattern() + "' found with with different index mode: " + m2 + " != " + m1
                );
            }
        });

        // CPS lenient lookups: collect ViewShadowRelation patterns. Shadows live as siblings of the
        // strict UnresolvedRelation inside per-resolution-level ViewUnionAlls (see ViewResolver).
        // A LinkedHashSet preserves the order shadows were emitted in for deterministic test output;
        // it also deduplicates so two shadows with the same indexPattern only produce one lenient call.
        Set<IndexPattern> viewShadows = new LinkedHashSet<>();
        plan.forEachUp(ViewShadowRelation.class, p -> viewShadows.add(p.optionalLinkedPattern()));

        List<Enrich> unresolvedEnriches = new ArrayList<>();
        plan.forEachUp(Enrich.class, unresolvedEnriches::add);

        // External source paths. Every tablePath is a non-null Literal post-parsing; non-Literal here
        // is a precondition violation and throws.
        List<String> icebergPaths = new ArrayList<>();
        plan.forEachUp(UnresolvedExternalRelation.class, p -> {
            if (p.tablePath() instanceof Literal literal && literal.value() != null) {
                String path = org.elasticsearch.common.lucene.BytesRefs.toString(literal.value());
                icebergPaths.add(path);
            } else {
                throw new IllegalStateException(
                    "UnresolvedExternalRelation tablePath is not a non-null Literal: ["
                        + (p.tablePath() == null ? "null" : p.tablePath().sourceText())
                        + "]"
                );
            }
        });

        /*
         * Enable aggregate_metric_double and dense_vector when we see certain functions
         * or the TS command. This allowed us to release these when not all nodes understand
         * these types. These functions are only supported on newer nodes, so we use them
         * as a signal that the query is only for nodes that support these types.
         *
         * This was a workaround that was required to enable these in 9.2.0. These days
         * we enable these field types if all nodes in all clusters support them. But this
         * work around persists to support force-enabling them on queries that might touch
         * nodes that don't have 9.2.1 or 9.3.0. If all nodes in the cluster have 9.2.1 or 9.3.0
         * this code doesn't do anything.
         */
        Holder<Boolean> useAggregateMetricDoubleWhenNotSupported = new Holder<>(false);
        Holder<Boolean> useDenseVectorWhenNotSupported = new Holder<>(false);
        indexes.forEach((ip, mode) -> {
            if (mode == IndexMode.TIME_SERIES) {
                useAggregateMetricDoubleWhenNotSupported.set(true);
            }
        });
        plan.forEachDown(p -> p.forEachExpression(UnresolvedFunction.class, fn -> {
            if (fn.name().equalsIgnoreCase("knn")
                || fn.name().equalsIgnoreCase("to_dense_vector")
                || fn.name().equalsIgnoreCase("v_cosine")
                || fn.name().equalsIgnoreCase("v_hamming")
                || fn.name().equalsIgnoreCase("v_l1_norm")
                || fn.name().equalsIgnoreCase("v_l2_norm")
                || fn.name().equalsIgnoreCase("v_dot_product")
                || fn.name().equalsIgnoreCase("v_magnitude")) {
                useDenseVectorWhenNotSupported.set(true);
            }
            if (fn.name().equalsIgnoreCase("to_aggregate_metric_double")) {
                useAggregateMetricDoubleWhenNotSupported.set(true);
            }
        }));

        Holder<Boolean> hasTimeSeriesAggregation = new Holder<>(false);
        plan.forEachUp(TimeSeriesAggregate.class, p -> hasTimeSeriesAggregation.set(true));
        plan.forEachUp(PromqlCommand.class, p -> hasTimeSeriesAggregation.set(true));

        // mark plan as preAnalyzed (if it were marked, there would be no analysis)
        plan.forEachUp(LogicalPlan::setPreAnalyzed);

        return new PreAnalysis(
            indexes,
            unresolvedEnriches,
            lookupIndices,
            viewShadows,
            useAggregateMetricDoubleWhenNotSupported.get(),
            useDenseVectorWhenNotSupported.get(),
            hasTimeSeriesAggregation.get(),
            icebergPaths
        );
    }
}
