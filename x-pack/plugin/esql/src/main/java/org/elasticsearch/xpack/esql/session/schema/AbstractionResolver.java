/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * The kind-blind front (the architecture's stage ①): resolve a {@code FROM} expression's patterns / exclusions /
 * selectors against the caller's authorized abstractions <em>once</em>, and classify each resulting concrete local
 * name by what it <em>is</em> — its {@link IndexAbstraction.Type} (CONCRETE_INDEX / ALIAS / DATA_STREAM / VIEW /
 * DATASET). It knows what each name is, nothing about what its kind <em>does</em>; the per-kind logic downstream
 * receives already-classified concrete names and never re-runs pattern matching.
 *
 * <p>This does not reimplement pattern logic — it wraps the server's {@link IndexAbstractionResolver}, which already
 * expands wildcards, applies exclusions in resolution order, honours selectors, and respects the
 * {@code resolveViews}/{@code resolveDatasets} visibility flags on the supplied {@link IndicesOptions}. The single
 * thing this front adds on top is the {@code (name -> IndexAbstraction.Type)} classification both the view and the
 * dataset resolution previously open-coded against {@code ProjectMetadata#getIndicesLookup} after resolving.
 *
 * <p>Behaviour-identical by construction: each consumer keeps passing its own {@link IndicesOptions} (so each kind
 * still resolves under its own visibility flags) and the classification reads the same {@code getIndicesLookup()} the
 * inline loops read. See {@code ViewResolutionService} and {@code DatasetRewriter} for the two consumers routed
 * through it.
 */
public final class AbstractionResolver {

    private final IndexAbstractionResolver delegate;

    public AbstractionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.delegate = new IndexAbstractionResolver(indexNameExpressionResolver);
    }

    /**
     * Resolve {@code patterns} against the authorized/available abstractions and classify each concrete local name by
     * kind. Data streams are always in scope ({@code includeDataStreams = true}) — a {@code FROM} expression resolves
     * over every namespace abstraction; which of them are actually returned is governed by the
     * {@code resolveViews}/{@code resolveDatasets} flags on {@code indicesOptions}, not by this front.
     */
    public Resolution resolve(
        List<String> patterns,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        Function<IndexComponentSelector, Set<String>> authorizedAndAvailableBySelector,
        BiPredicate<String, IndexComponentSelector> isAuthorized
    ) {
        ResolvedIndexExpressions expressions = delegate.resolveIndexAbstractions(
            patterns,
            indicesOptions,
            projectMetadata,
            authorizedAndAvailableBySelector,
            isAuthorized,
            true
        );
        return classify(expressions, projectMetadata);
    }

    /**
     * Classify already-resolved expressions (e.g. those a security filter pre-computed onto the request) without
     * re-running resolution — same classification, no second resolve call.
     */
    public static Resolution classify(ResolvedIndexExpressions expressions, ProjectMetadata projectMetadata) {
        return new Resolution(expressions, projectMetadata.getIndicesLookup());
    }

    /**
     * One {@code FROM} expression's resolution: the raw {@link ResolvedIndexExpressions} (callers that need positional
     * / per-expression detail — exclusion ordering, CPS linked-relation generation — still read this) plus the
     * by-kind classification of its concrete local names.
     */
    public record Resolution(ResolvedIndexExpressions expressions, Map<String, IndexAbstraction> lookup) {

        /** The resolved concrete local names, in resolution order. */
        public List<String> localNames() {
            return expressions.getLocalIndicesList();
        }

        /** The resolved concrete local names of the given kind, in resolution order. Names absent from the lookup
         *  (e.g. a synthesized date-math name) are not of any kind and are skipped. */
        public List<String> namesOfKind(IndexAbstraction.Type kind) {
            List<String> result = new ArrayList<>();
            for (String name : localNames()) {
                IndexAbstraction abstraction = lookup.get(name);
                if (abstraction != null && abstraction.getType() == kind) {
                    result.add(name);
                }
            }
            return result;
        }

        /** The resolved abstractions of the given kind, in resolution order. */
        public List<IndexAbstraction> abstractionsOfKind(IndexAbstraction.Type kind) {
            List<IndexAbstraction> result = new ArrayList<>();
            for (String name : localNames()) {
                IndexAbstraction abstraction = lookup.get(name);
                if (abstraction != null && abstraction.getType() == kind) {
                    result.add(abstraction);
                }
            }
            return result;
        }

        /** Whether any resolved concrete local name is of a kind other than {@code kind} (lookup-absent names skipped). */
        public boolean hasKindOtherThan(IndexAbstraction.Type kind) {
            for (String name : localNames()) {
                IndexAbstraction abstraction = lookup.get(name);
                if (abstraction != null && abstraction.getType() != kind) {
                    return true;
                }
            }
            return false;
        }
    }
}
