/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ReplacedIndexExpression;
import org.elasticsearch.action.ReplacedIndexExpressions;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.UnsupportedSelectorException;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

public class IndexAbstractionResolver {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexAbstractionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public ReplacedIndexExpressions resolveIndexAbstractionsForCrossProjectSearch(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        BiPredicate<String, IndexComponentSelector> isAuthorized,
        boolean includeDataStreams
    ) {
        return resolveIndexAbstractions(
            indices,
            // lenient mode required to avoid failing if an index exists in another project
            lenientIndicesOptions(indicesOptions),
            projectMetadata,
            allAuthorizedAndAvailableBySelector,
            isAuthorized,
            includeDataStreams
        );
    }

    public ReplacedIndexExpressions resolveIndexAbstractions(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        BiPredicate<String, IndexComponentSelector> isAuthorized,
        boolean includeDataStreams
    ) {
        final Map<String, ReplacedIndexExpression> replaced = new LinkedHashMap<>();

        boolean wildcardSeen = false;

        for (String originalIndexExpression : indices) {
            String indexAbstraction;
            boolean minus = false;

            if (originalIndexExpression.charAt(0) == '-' && wildcardSeen) {
                indexAbstraction = originalIndexExpression.substring(1);
                minus = true;
            } else {
                indexAbstraction = originalIndexExpression;
            }

            final Tuple<String, String> expressionAndSelector = IndexNameExpressionResolver.splitSelectorExpression(indexAbstraction);
            final String selectorString = expressionAndSelector.v2();
            if (indicesOptions.allowSelectors() == false && selectorString != null) {
                throw new UnsupportedSelectorException(indexAbstraction);
            }

            indexAbstraction = expressionAndSelector.v1();
            final IndexComponentSelector selector = IndexComponentSelector.getByKeyOrThrow(selectorString);

            indexAbstraction = IndexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);

            final Set<String> resolvedForThisInput = new LinkedHashSet<>();

            if (indicesOptions.expandWildcardExpressions() && Regex.isSimpleMatchPattern(indexAbstraction)) {
                wildcardSeen = true;

                final Set<String> resolvedIndices = new LinkedHashSet<>();
                for (String authorizedIndex : allAuthorizedAndAvailableBySelector.apply(selector)) {
                    if (Regex.simpleMatch(indexAbstraction, authorizedIndex)
                        && isIndexVisible(
                            indexAbstraction,
                            selectorString,
                            authorizedIndex,
                            indicesOptions,
                            projectMetadata,
                            indexNameExpressionResolver,
                            includeDataStreams
                        )) {
                        resolveSelectorsAndCollect(authorizedIndex, selectorString, indicesOptions, resolvedIndices, projectMetadata);
                    }
                }

                if (resolvedIndices.isEmpty()) {
                    if (indicesOptions.allowNoIndices() == false) {
                        throw new IndexNotFoundException(indexAbstraction);
                    }
                }

                if (minus) {
                    if (resolvedIndices.isEmpty() == false) {
                        for (ReplacedIndexExpression prior : replaced.values()) {
                            if (prior.replacedBy().isEmpty()) {
                                continue;
                            }
                            prior.replacedBy().removeAll(resolvedIndices);
                        }
                    }
                } else {
                    resolvedForThisInput.addAll(resolvedIndices);
                    replaced.put(
                        originalIndexExpression,
                        new ReplacedIndexExpression(
                            originalIndexExpression,
                            new ArrayList<>(resolvedForThisInput),
                            ReplacedIndexExpression.ResolutionResult.SUCCESS,
                            null
                        )
                    );
                }
            } else {
                final Set<String> resolvedIndices = new LinkedHashSet<>();
                resolveSelectorsAndCollect(indexAbstraction, selectorString, indicesOptions, resolvedIndices, projectMetadata);

                if (minus) {
                    if (resolvedIndices.isEmpty() == false) {
                        for (ReplacedIndexExpression prior : replaced.values()) {
                            if (prior.replacedBy().isEmpty()) {
                                continue;
                            }
                            prior.replacedBy().removeAll(resolvedIndices);
                        }
                    }
                } else {
                    // We should consider if this needs to be optimized to avoid checking authorization and existence here
                    final boolean authorized = isAuthorized.test(indexAbstraction, selector);
                    final boolean existsAndVisible = authorized
                        && existsAndVisible(indicesOptions, projectMetadata, includeDataStreams, indexAbstraction, selectorString);
                    final ReplacedIndexExpression.ResolutionResult resolutionResult = existsAndVisible
                        ? ReplacedIndexExpression.ResolutionResult.SUCCESS
                        : (authorized
                            ? ReplacedIndexExpression.ResolutionResult.CONCRETE_RESOURCE_MISSING
                            : ReplacedIndexExpression.ResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED);
                    if (indicesOptions.ignoreUnavailable() == false || authorized) {
                        resolvedForThisInput.addAll(resolvedIndices);
                    }
                    replaced.put(
                        originalIndexExpression,
                        new ReplacedIndexExpression(originalIndexExpression, new ArrayList<>(resolvedForThisInput), resolutionResult, null)
                    );
                }
            }
        }

        return new ReplacedIndexExpressions(replaced);
    }

    private static void resolveSelectorsAndCollect(
        String indexAbstraction,
        String selectorString,
        IndicesOptions indicesOptions,
        Set<String> collect,
        ProjectMetadata projectMetadata
    ) {
        if (indicesOptions.allowSelectors()) {
            IndexAbstraction abstraction = projectMetadata.getIndicesLookup().get(indexAbstraction);
            // We can't determine which selectors are valid for a nonexistent abstraction, so simply propagate them as if they supported
            // all of them so we don't drop anything.
            boolean acceptsAllSelectors = abstraction == null || abstraction.isDataStreamRelated();

            // Supply default if needed
            if (selectorString == null) {
                selectorString = IndexComponentSelector.DATA.getKey();
            }

            // A selector is always passed along as-is, it's validity for this kind of abstraction is tested later
            collect.add(IndexNameExpressionResolver.combineSelectorExpression(indexAbstraction, selectorString));
        } else {
            assert selectorString == null
                : "A selector string [" + selectorString + "] is present but selectors are disabled in this context";
            collect.add(indexAbstraction);
        }
    }

    public static boolean isIndexVisible(
        String expression,
        @Nullable String selectorString,
        String index,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver resolver,
        boolean includeDataStreams
    ) {
        IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(index);
        if (indexAbstraction == null) {
            throw new IllegalStateException("could not resolve index abstraction [" + index + "]");
        }
        final boolean isHidden = indexAbstraction.isHidden();
        boolean isVisible = isHidden == false || indicesOptions.expandWildcardsHidden() || isVisibleDueToImplicitHidden(expression, index);
        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
            // it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
            // it's complicated to support those options with aliases pointing to multiple indices...
            isVisible = isVisible && indicesOptions.ignoreAliases() == false;

            if (isVisible && indexAbstraction.isSystem()) {
                // check if it is net new
                if (resolver.getNetNewSystemIndexPredicate().test(indexAbstraction.getName())) {
                    // don't give this code any particular credit for being *correct*. it's just trying to resolve a combination of
                    // issues in a way that happens to *work*. there's probably a better way of writing things such that this won't
                    // be necessary, but for the moment, it happens to be expedient to write things this way.

                    // unwrap the alias and re-run the function on the write index of the alias -- that is, the alias is visible if
                    // the concrete index that it refers to is visible
                    Index writeIndex = indexAbstraction.getWriteIndex();
                    if (writeIndex == null) {
                        return false;
                    } else {
                        return isIndexVisible(
                            expression,
                            selectorString,
                            writeIndex.getName(),
                            indicesOptions,
                            projectMetadata,
                            resolver,
                            includeDataStreams
                        );
                    }
                }
            }

            if (isVisible && selectorString != null) {
                // Check if a selector was present, and if it is, check if this alias is applicable to it
                IndexComponentSelector selector = IndexComponentSelector.getByKey(selectorString);
                if (IndexComponentSelector.FAILURES.equals(selector)) {
                    isVisible = indexAbstraction.isDataStreamRelated();
                }
            }
            return isVisible;
        }
        if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
            if (includeDataStreams == false) {
                return false;
            }
            if (indexAbstraction.isSystem()) {
                return isSystemIndexVisible(resolver, indexAbstraction);
            } else {
                return isVisible;
            }
        }
        assert indexAbstraction.getIndices().size() == 1 : "concrete index must point to a single index";
        if (isVisible == false) {
            return false;
        }
        if (indexAbstraction.isSystem()) {
            // check if it is net new
            if (resolver.getNetNewSystemIndexPredicate().test(indexAbstraction.getName())) {
                return isSystemIndexVisible(resolver, indexAbstraction);
            }

            // does the system index back a system data stream?
            if (indexAbstraction.getParentDataStream() != null) {
                if (indexAbstraction.getParentDataStream().isSystem() == false) {
                    assert false : "system index is part of a data stream that is not a system data stream";
                    throw new IllegalStateException("system index is part of a data stream that is not a system data stream");
                }
                return isSystemIndexVisible(resolver, indexAbstraction);
            }
        }
        if (selectorString != null && Regex.isMatchAllPattern(selectorString) == false) {
            // Check if a selector was present, and if it is, check if this index is applicable to it
            IndexComponentSelector selector = IndexComponentSelector.getByKey(selectorString);
            if (IndexComponentSelector.FAILURES.equals(selector)) {
                return false;
            }
        }

        IndexMetadata indexMetadata = projectMetadata.index(indexAbstraction.getIndices().get(0));
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.OPEN && indicesOptions.expandWildcardsOpen()) {
            return true;
        }
        return false;
    }

    private static boolean isSystemIndexVisible(IndexNameExpressionResolver resolver, IndexAbstraction indexAbstraction) {
        final SystemIndexAccessLevel level = resolver.getSystemIndexAccessLevel();
        switch (level) {
            case ALL:
                return true;
            case NONE:
                return false;
            case RESTRICTED:
                return resolver.getSystemIndexAccessPredicate().test(indexAbstraction.getName());
            case BACKWARDS_COMPATIBLE_ONLY:
                return resolver.getNetNewSystemIndexPredicate().test(indexAbstraction.getName());
            default:
                assert false : "unexpected system index access level [" + level + "]";
                throw new IllegalStateException("unexpected system index access level [" + level + "]");
        }
    }

    private boolean existsAndVisible(
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        boolean includeDataStreams,
        String indexAbstraction,
        String selectorString
    ) {
        var abstraction = projectMetadata.getIndicesLookup().get(indexAbstraction);
        return abstraction != null
            && isIndexVisible(
                indexAbstraction,
                selectorString,
                indexAbstraction,
                indicesOptions,
                projectMetadata,
                indexNameExpressionResolver,
                includeDataStreams
            );
    }

    private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
        return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
    }

    private static IndicesOptions lenientIndicesOptions(IndicesOptions indicesOptions) {
        return IndicesOptions.builder(indicesOptions)
            .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true))
            .wildcardOptions(IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build())
            .build();
    }
}
