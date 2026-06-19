/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.Embedding;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is part of the planner.  Acts somewhat like a linker, to find the indices and enrich policies referenced by the query.
 */
public class PreAnalyzer {

    /**
     * The list of inference function definitions that are supported by the PreAnalyzer.
     */
    static final List<FunctionDefinition> INFERENCE_FUNCTION_DEFINITIONS = List.of(TextEmbedding.DEFINITION, Embedding.DEFINITION);

    public record PreAnalysis(
        Map<IndexPattern, IndexMode> indexes,
        List<Enrich> enriches,
        List<IndexPattern> lookupIndices,
        // Maps each lookup index pattern to the main (non-lookup) index patterns its LOOKUP JOIN must be resolvable against. A lookup
        // nested inside a subquery maps to that subquery's main patterns; a lookup in the main query (which runs after the subqueries are
        // unioned) maps to the whole query's main patterns - the union of the top-level main FROM and every subquery's main patterns. This
        // lets index resolution require the lookup index only on the relevant clusters rather than on every cluster involved in the overall
        // query. A lookup index that resolves to an empty set of main patterns is treated as referencing all clusters.
        Map<IndexPattern, Set<IndexPattern>> lookupIndexPatternsToMainAndSubqueryIndexPatterns,
        Set<LinkedIndexPattern> linkedIndices,  // CPS only, patterns from local view names that could match remote indices
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        List<String> icebergPaths,
        List<String> inferenceIds
    ) {
        public static final PreAnalysis EMPTY = new PreAnalysis(
            Map.of(),
            List.of(),
            List.of(),
            Map.of(),
            Set.of(),
            false,
            false,
            false,
            List.of(),
            List.of()
        );
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

        Map<IndexPattern, Set<IndexPattern>> lookupIndexPatternsToMainAndSubqueryIndexPatterns = mapLookupIndicesToSubqueryMainPatterns(
            plan
        );

        // CPS: collect ViewShadowRelation patterns. Shadows live as siblings of the
        // strict UnresolvedRelation inside per-resolution-level ViewUnionAlls (see ViewResolver).
        // A LinkedHashSet preserves the order shadows were emitted in for deterministic test output;
        // it also deduplicates so two shadows with the same indexPattern only produce one lenient call.
        Set<LinkedIndexPattern> linkedIndexPatterns = new LinkedHashSet<>();
        plan.forEachUp(ViewShadowRelation.class, p -> linkedIndexPatterns.add(p.linkedIndexPattern()));

        List<Enrich> unresolvedEnriches = new ArrayList<>();
        plan.forEachUp(Enrich.class, unresolvedEnriches::add);

        // External source paths. Every tablePath is a non-null Literal post-parsing; non-Literal here
        // is a precondition violation and throws.
        List<String> icebergPaths = new ArrayList<>();
        plan.forEachUp(UnresolvedExternalRelation.class, p -> {
            if (p.tablePath() instanceof Literal literal && literal.value() != null) {
                String path = BytesRefs.toString(literal.value());
                icebergPaths.add(path);
            } else {
                throw new IllegalStateException(
                    "UnresolvedExternalRelation tablePath is not a non-null Literal: ["
                        + (p.tablePath() == null ? "null" : p.tablePath().sourceText())
                        + "]"
                );
            }
        });

        List<String> inferenceIds = new ArrayList<>();
        // Inference commands require a literal inference_id at parse time, unlike
        // UnresolvedFunction calls where the ID may be dynamic.
        plan.forEachUp(InferencePlan.class, inferencePlan -> inferenceIds.add(inferenceId(inferencePlan)));

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
            FunctionDefinition inferenceFunction = inferenceFunctionDefinition(fn.name());
            if (inferenceFunction != null) {
                String inferenceId = inferenceId(fn, inferenceFunction);
                if (inferenceId != null) {
                    inferenceIds.add(inferenceId);
                }
            }
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
            lookupIndexPatternsToMainAndSubqueryIndexPatterns,
            linkedIndexPatterns,
            useAggregateMetricDoubleWhenNotSupported.get(),
            useDenseVectorWhenNotSupported.get(),
            hasTimeSeriesAggregation.get(),
            icebergPaths,
            inferenceIds
        );
    }

    /**
     * Associates each lookup index with the main (non-lookup) index patterns its {@code LOOKUP JOIN} must be resolvable against.
     * <p>
     * A {@code LOOKUP JOIN} only needs its lookup index to exist on the clusters whose data actually reaches the join. By recording, per
     * lookup index, the relevant main index patterns, index resolution can later limit the lookup index requirement to just those clusters
     * instead of every cluster in the overall query.
     * <p>
     * The query is processed as a tree of <em>scopes</em>: the whole query is the outermost scope and each {@link Subquery} introduces a
     * nested scope. A scope's main patterns are the main {@link UnresolvedRelation}s it directly owns plus the main patterns of every scope
     * nested within it, because those nested subqueries' outputs are unioned into this scope before any of this scope's own
     * {@code LOOKUP JOIN}s run. Each lookup index is associated with the main patterns of the scope that <em>directly</em> contains it - so
     * a deeply nested lookup is scoped to its own (inner) data, while a main-query lookup spans the whole query. The same lookup index name
     * can appear in more than one scope; when that happens the entries are merged by union, because the lookup index must then be present
     * on the clusters of all of them.
     * <p>
     * An {@code IN}/{@code NOT IN} subquery (resolved into an {@link AbstractSubqueryJoin}: {@code SemiJoin}/{@code AntiJoin}/
     * {@code MarkJoin}) is treated differently from a {@code FROM} {@link Subquery}. Its right side is an independent scope - lookups
     * inside it are scoped to that subquery's own main patterns - but its data only filters the enclosing scope's rows; it is never unioned
     * into the enclosing scope. Its main patterns are therefore <em>not</em> folded into the enclosing scope, so a lookup in the enclosing
     * scope is never required on the (possibly remote) clusters referenced only inside the {@code IN}/{@code NOT IN} subquery.
     */
    private static Map<IndexPattern, Set<IndexPattern>> mapLookupIndicesToSubqueryMainPatterns(LogicalPlan plan) {
        Map<IndexPattern, Set<IndexPattern>> lookupIndexPatternsToMainPatterns = new HashMap<>();
        collectScopeMainPatterns(plan, lookupIndexPatternsToMainPatterns);
        return lookupIndexPatternsToMainPatterns;
    }

    /**
     * Processes a single scope - the whole query, the body of one {@link Subquery}, or the right side of one {@link AbstractSubqueryJoin}
     * ({@code IN}/{@code NOT IN} subquery) - recording every lookup index directly in that scope against the scope's main patterns, and
     * returns those main patterns so the enclosing scope can fold them into its own.
     * <p>
     * The scope's main patterns are the main relations it directly owns (those not below a nested {@link Subquery} or
     * {@link AbstractSubqueryJoin}) together with the main patterns of every nested {@code FROM} subquery, because a {@code FROM}
     * subquery's output is unioned into this scope before this scope's own {@code LOOKUP JOIN}s run. The main patterns of a nested
     * {@code IN}/{@code NOT IN} subquery are deliberately excluded: that subquery only filters this scope's rows, its data is never unioned
     * in, so it must not pull this scope's lookups onto the clusters it references.
     */
    private static Set<IndexPattern> collectScopeMainPatterns(
        LogicalPlan scopeRoot,
        Map<IndexPattern, Set<IndexPattern>> lookupIndexPatternsToMainPatterns
    ) {
        Set<IndexPattern> directMainPatterns = new HashSet<>();
        Set<IndexPattern> directLookupPatterns = new HashSet<>();
        List<Subquery> nestedSubqueries = new ArrayList<>();
        List<LogicalPlan> inSubqueries = new ArrayList<>();
        collectScopeNodes(scopeRoot, directMainPatterns, directLookupPatterns, nestedSubqueries, inSubqueries);

        // This scope's main patterns include its directly-owned mains plus every nested FROM subquery's main patterns (those nested
        // outputs are unioned into this scope before this scope's own lookups run). IN/NOT IN subqueries are intentionally not folded in.
        Set<IndexPattern> scopeMainPatterns = new HashSet<>(directMainPatterns);
        for (Subquery nestedSubquery : nestedSubqueries) {
            scopeMainPatterns.addAll(collectScopeMainPatterns(nestedSubquery.plan(), lookupIndexPatternsToMainPatterns));
        }

        // IN/NOT IN subqueries are independent scopes: process them so their own nested lookups get scoped to their own main patterns, but
        // discard the returned main patterns - a lookup in this scope must not be required on the clusters referenced only inside them.
        for (LogicalPlan inSubquery : inSubqueries) {
            collectScopeMainPatterns(inSubquery, lookupIndexPatternsToMainPatterns);
        }

        // The lookups directly in this scope run on this scope's (unioned) data, so they map to this scope's main patterns.
        mergeLookupToMainPatterns(lookupIndexPatternsToMainPatterns, directLookupPatterns, scopeMainPatterns);

        return scopeMainPatterns;
    }

    /**
     * Walks a single scope rooted at {@code node}, classifying the nodes it owns into this scope's main / lookup relations and the nested
     * scopes it introduces, stopping at each scope boundary:
     * <ul>
     *   <li>an {@link UnresolvedRelation} is a leaf collected as a main or lookup pattern of this scope;</li>
     *   <li>a {@link Subquery} ({@code FROM} subquery) is a nested folding scope - it is recorded but not descended into, so its relations
     *   are processed by the recursive call on its body;</li>
     *   <li>an {@link AbstractSubqueryJoin} ({@code IN}/{@code NOT IN} subquery) is split: its left side continues this scope (descended
     *   here), while its right side is the independent, non-folding subquery scope recorded in {@code inSubqueries};</li>
     *   <li>any other node stays within this scope, so the walk descends into all of its children.</li>
     * </ul>
     */
    private static void collectScopeNodes(
        LogicalPlan node,
        Set<IndexPattern> directMainPatterns,
        Set<IndexPattern> directLookupPatterns,
        List<Subquery> nestedSubqueries,
        List<LogicalPlan> inSubqueries
    ) {
        if (node instanceof UnresolvedRelation relation) {
            collectMainOrLookupPattern(relation, directMainPatterns, directLookupPatterns);
        } else if (node instanceof Subquery subquery) {
            nestedSubqueries.add(subquery);
        } else if (node instanceof AbstractSubqueryJoin subqueryJoin) {
            // The left side is the main query the IN/NOT IN filter applies to (same scope); the right side is the independent subquery.
            inSubqueries.add(subqueryJoin.right());
            collectScopeNodes(subqueryJoin.left(), directMainPatterns, directLookupPatterns, nestedSubqueries, inSubqueries);
        } else {
            for (LogicalPlan child : node.children()) {
                collectScopeNodes(child, directMainPatterns, directLookupPatterns, nestedSubqueries, inSubqueries);
            }
        }
    }

    private static void collectMainOrLookupPattern(
        UnresolvedRelation relation,
        Set<IndexPattern> mainPatterns,
        Set<IndexPattern> lookupPatterns
    ) {
        if (relation.indexMode() == IndexMode.LOOKUP) {
            lookupPatterns.add(relation.indexPattern());
        } else {
            mainPatterns.add(relation.indexPattern());
        }
    }

    /**
     * Records, for a single scope, the association from each of that scope's {@code lookupPatterns} to that scope's {@code mainPatterns},
     * accumulating into {@code lookupIndexPatternsToMainPatterns} across all scopes.
     * <p>
     * For every lookup index in the scope, {@link Map#merge} inserts {@code mainPatterns} the first time the lookup index is seen, and
     * unions it with the already-recorded patterns when the same lookup index also appears in another scope (so the lookup index is
     * required on the clusters of <em>all</em> scopes that use it).
     * <p>
     * Worked example for the query:
     * <pre>{@code
     *   FROM main0,                                                  // top-level main FROM
     *     (FROM m1 | LOOKUP JOIN lk1 ON f),                          // subquery A
     *     (FROM m2, m3 | LOOKUP JOIN lk1 ON f | LOOKUP JOIN lk2 ON g)   // subquery B
     *   | LOOKUP JOIN lk0 ON h                                       // main-query lookup
     * }</pre>
     * The subqueries are merged first; the main-query lookup is then merged against the whole query's main patterns - the union of the
     * top-level main FROM and every subquery's main patterns:
     * <pre>{@code
     *   // subquery A:  mainPatterns={m1},      lookupPatterns={lk1}
     *   merge(lk1, {m1})     -> { lk1={m1} }
     *
     *   // subquery B:  mainPatterns={m2, m3},  lookupPatterns={lk1, lk2}
     *   merge(lk1, {m2, m3}) -> { lk1={m1, m2, m3} }              // lk1 already present: union with existing {m1}
     *   merge(lk2, {m2, m3}) -> { lk1={m1, m2, m3}, lk2={m2, m3} }
     *
     *   // main query:  lookupPatterns={lk0}, mainPatterns={main0} + {m1} + {m2, m3} = {main0, m1, m2, m3}
     *   merge(lk0, {main0, m1, m2, m3}) -> { lk1={m1, m2, m3}, lk2={m2, m3}, lk0={main0, m1, m2, m3} }
     * }</pre>
     * Final result: {@code { lk0={main0, m1, m2, m3}, lk1={m1, m2, m3}, lk2={m2, m3} }}. lk0 (a main-query lookup) spans every main
     * pattern because it runs on the unioned output; lk1 spans both subqueries' main patterns because it is used in both; lk2 is scoped to
     * subquery B's main patterns only.
     */
    private static void mergeLookupToMainPatterns(
        Map<IndexPattern, Set<IndexPattern>> lookupIndexPatternsToMainPatterns,
        Set<IndexPattern> lookupPatterns,
        Set<IndexPattern> mainPatterns
    ) {
        for (IndexPattern lookupPattern : lookupPatterns) {
            lookupIndexPatternsToMainPatterns.merge(lookupPattern, mainPatterns, (existing, added) -> {
                Set<IndexPattern> merged = new HashSet<>(existing);
                merged.addAll(added);
                return merged;
            });
        }
    }

    private static String inferenceId(InferencePlan<?> plan) {
        return BytesRefs.toString(plan.inferenceId().fold(FoldContext.small()));
    }

    private static FunctionDefinition inferenceFunctionDefinition(String name) {
        for (FunctionDefinition def : INFERENCE_FUNCTION_DEFINITIONS) {
            if (name.equalsIgnoreCase(def.name())) {
                return def;
            }
        }
        return null;
    }

    private static String inferenceId(UnresolvedFunction func, FunctionDefinition def) {
        EsqlFunctionRegistry.FunctionDescription functionDescription = EsqlFunctionRegistry.description(def);

        for (int i = 0; i < functionDescription.args().size(); i++) {
            EsqlFunctionRegistry.ArgSignature arg = functionDescription.args().get(i);
            if (i >= func.arguments().size()) {
                // Argument is missing. We will fail later during verifier, so just return null here.
                return null;
            }

            if (arg.name().equals(InferenceFunction.INFERENCE_ID_PARAMETER_NAME)) {
                Expression inferenceId = func.arguments().get(i);
                if (inferenceId != null && inferenceId.foldable() && DataType.isString(inferenceId.dataType())) {
                    return BytesRefs.toString(inferenceId.fold(FoldContext.small()));
                }
            }
        }

        return null;
    }
}
