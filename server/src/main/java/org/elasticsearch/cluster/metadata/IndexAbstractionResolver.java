/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.CrossProjectUtils;
import org.elasticsearch.action.IndicesRequest;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static org.elasticsearch.action.IndicesRequest.ReplacedExpression.hasCanonicalExpressionForOrigin;

public class IndexAbstractionResolver {

    private static final Logger logger = LogManager.getLogger(IndexAbstractionResolver.class);

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexAbstractionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public Map<String, IndicesRequest.ReplacedExpression> resolveIndexAbstractionsCrossProject(
        Map<String, IndicesRequest.ReplacedExpression> rewrittenExpressions,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        BiPredicate<String, IndexComponentSelector> isAuthorized,
        boolean includeDataStreams
    ) {
        // TODO handle exclusions
        // TODO consolidate with `resolveIndexAbstractions` somehow, maybe?
        Map<String, IndicesRequest.ReplacedExpression> finalReplacedExpressions = new LinkedHashMap<>();
        for (IndicesRequest.ReplacedExpression replacedExpression : rewrittenExpressions.values()) {
            // no expressions targeting origin, nothing to rewrite
            if (false == hasCanonicalExpressionForOrigin(replacedExpression.replacedBy())) {
                logger.info(
                    "Skipping rewriting of qualified expression [{}] because there are no origin expressions",
                    replacedExpression.original()
                );
                finalReplacedExpressions.put(replacedExpression.original(), replacedExpression);
                continue;
            }

            String indexAbstraction = replacedExpression.original();

            // Always check to see if there's a selector on the index expression
            Tuple<String, String> expressionAndSelector = IndexNameExpressionResolver.splitSelectorExpression(indexAbstraction);
            String selectorString = expressionAndSelector.v2();
            if (indicesOptions.allowSelectors() == false && selectorString != null) {
                throw new UnsupportedSelectorException(indexAbstraction);
            }
            indexAbstraction = expressionAndSelector.v1();
            IndexComponentSelector selector = IndexComponentSelector.getByKeyOrThrow(selectorString);

            // we always need to check for date math expressions
            indexAbstraction = IndexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);

            if (indicesOptions.expandWildcardExpressions() && Regex.isSimpleMatchPattern(indexAbstraction)) {
                Set<String> resolvedIndices = new HashSet<>();
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
                finalReplacedExpressions.put(replacedExpression.original(), replaceOriginIndices(replacedExpression, resolvedIndices));
            } else {
                Set<String> resolvedIndices = new HashSet<>();
                resolveSelectorsAndCollect(indexAbstraction, selectorString, indicesOptions, resolvedIndices, projectMetadata);

                // TODO not quite right: we need to record if we didn't have access for the fan-out action to throw
                boolean authorized = isAuthorized.test(indexAbstraction, selector);
                if (authorized) {
                    var visible = existsAndVisible(indicesOptions, projectMetadata, includeDataStreams, indexAbstraction, selectorString);
                    finalReplacedExpressions.put(
                        replacedExpression.original(),
                        replaceOriginIndices(replacedExpression, visible ? resolvedIndices : Set.of())
                    );
                } else {
                    finalReplacedExpressions.put(replacedExpression.original(), replaceOriginIndices(replacedExpression, Set.of()));
                }

            }
        }
        logger.info("Rewrote expressions from [{}] to [{}]", rewrittenExpressions, finalReplacedExpressions);
        assert finalReplacedExpressions.size() == rewrittenExpressions.size()
            : "finalRewrittenExpressions size ["
                + finalReplacedExpressions.size()
                + "] does not match original size ["
                + rewrittenExpressions.size()
                + "]";
        return finalReplacedExpressions;
    }

    private static IndicesRequest.ReplacedExpression replaceOriginIndices(
        IndicesRequest.ReplacedExpression rewrittenExpression,
        Set<String> resolvedIndices
    ) {
        logger.info("Replacing origin indices for expression [{}] with [{}]", rewrittenExpression, resolvedIndices);
        List<String> qualifiedExpressions = rewrittenExpression.replacedBy()
            .stream()
            .filter(CrossProjectUtils::isQualifiedIndexExpression)
            .toList();
        List<String> combined = new ArrayList<>(resolvedIndices);
        combined.addAll(qualifiedExpressions);
        var e = new IndicesRequest.ReplacedExpression(rewrittenExpression.original(), combined);
        logger.info("Replaced origin indices for expression [{}] with [{}]", rewrittenExpression, e);
        return e;
    }

    public Map<String, IndicesRequest.ReplacedExpression> resolveIndexAbstractionsMapping(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        BiPredicate<String, IndexComponentSelector> isAuthorized,
        boolean includeDataStreams
    ) {
        if (indicesOptions.ignoreUnavailable() == false) {
            throw new IllegalArgumentException("`ignoreUnavailable` must be true for this resolver");
        }

        if (indicesOptions.allowNoIndices() == false) {
            throw new IllegalArgumentException("`allowNoIndices` must be true for this resolver");
        }

        final Map<String, IndicesRequest.ReplacedExpression> resolutionMap = new LinkedHashMap<>();

        boolean wildcardSeen = false;

        for (String originalToken : indices) {
            boolean unauthorized = false;
            String indexAbstraction;
            boolean minus = false;

            if (originalToken.charAt(0) == '-' && wildcardSeen) {
                indexAbstraction = originalToken.substring(1);
                minus = true;
            } else {
                indexAbstraction = originalToken;
            }

            // Always check to see if there's a selector on the index expression
            final Tuple<String, String> expressionAndSelector = IndexNameExpressionResolver.splitSelectorExpression(indexAbstraction);
            final String selectorString = expressionAndSelector.v2();
            if (indicesOptions.allowSelectors() == false && selectorString != null) {
                throw new UnsupportedSelectorException(indexAbstraction);
            }
            indexAbstraction = expressionAndSelector.v1();
            final IndexComponentSelector selector = IndexComponentSelector.getByKeyOrThrow(selectorString);

            // Always handle date math
            indexAbstraction = IndexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);

            final Set<String> resolvedSet = new LinkedHashSet<>();

            if (indicesOptions.expandWildcardExpressions() && Regex.isSimpleMatchPattern(indexAbstraction)) {
                wildcardSeen = true;

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
                        resolveSelectorsAndCollect(authorizedIndex, selectorString, indicesOptions, resolvedSet, projectMetadata);
                    }
                }
            } else {
                // Non-wildcard path
                resolveSelectorsAndCollect(indexAbstraction, selectorString, indicesOptions, resolvedSet, projectMetadata);

                if (false == minus) {
                    boolean authorized = isAuthorized.test(indexAbstraction, selector);
                    if (false == authorized
                        || false == existsAndVisible(
                            indicesOptions,
                            projectMetadata,
                            includeDataStreams,
                            indexAbstraction,
                            selectorString
                        )) {
                        unauthorized = false == authorized;
                        resolvedSet.clear();
                    }
                }
            }

            if (false == resolvedSet.isEmpty()) {
                if (minus) {
                    for (var entry : resolutionMap.entrySet()) {
                        entry.getValue().replacedBy().removeAll(resolvedSet);
                    }
                } else {
                    resolutionMap.put(originalToken, new IndicesRequest.ReplacedExpression(originalToken, new ArrayList<>(resolvedSet)));
                }
            } else {
                resolutionMap.put(
                    originalToken,
                    new IndicesRequest.ReplacedExpression(originalToken, new ArrayList<>(resolvedSet), unauthorized, null)
                );
            }
        }

        return resolutionMap;
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

    public List<String> resolveIndexAbstractions(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        BiPredicate<String, IndexComponentSelector> isAuthorized,
        boolean includeDataStreams
    ) {
        List<String> finalIndices = new ArrayList<>();
        boolean wildcardSeen = false;
        for (String index : indices) {
            String indexAbstraction;
            boolean minus = false;
            if (index.charAt(0) == '-' && wildcardSeen) {
                indexAbstraction = index.substring(1);
                minus = true;
            } else {
                indexAbstraction = index;
            }

            // Always check to see if there's a selector on the index expression
            Tuple<String, String> expressionAndSelector = IndexNameExpressionResolver.splitSelectorExpression(indexAbstraction);
            String selectorString = expressionAndSelector.v2();
            if (indicesOptions.allowSelectors() == false && selectorString != null) {
                throw new UnsupportedSelectorException(indexAbstraction);
            }
            indexAbstraction = expressionAndSelector.v1();
            IndexComponentSelector selector = IndexComponentSelector.getByKeyOrThrow(selectorString);

            // we always need to check for date math expressions
            indexAbstraction = IndexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);

            if (indicesOptions.expandWildcardExpressions() && Regex.isSimpleMatchPattern(indexAbstraction)) {
                wildcardSeen = true;
                Set<String> resolvedIndices = new HashSet<>();
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
                    // es core honours allow_no_indices for each wildcard expression, we do the same here by throwing index not found.
                    if (indicesOptions.allowNoIndices() == false) {
                        throw new IndexNotFoundException(indexAbstraction);
                    }
                } else {
                    if (minus) {
                        finalIndices.removeAll(resolvedIndices);
                    } else {
                        finalIndices.addAll(resolvedIndices);
                    }
                }
            } else {
                Set<String> resolvedIndices = new HashSet<>();
                resolveSelectorsAndCollect(indexAbstraction, selectorString, indicesOptions, resolvedIndices, projectMetadata);
                if (minus) {
                    finalIndices.removeAll(resolvedIndices);
                } else if (indicesOptions.ignoreUnavailable() == false || isAuthorized.test(indexAbstraction, selector)) {
                    // Unauthorized names are considered unavailable, so if `ignoreUnavailable` is `true` they should be silently
                    // discarded from the `finalIndices` list. Other "ways of unavailable" must be handled by the action
                    // handler, see: https://github.com/elastic/elasticsearch/issues/90215
                    finalIndices.addAll(resolvedIndices);

                }
            }
        }
        return finalIndices;
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

    private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
        return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
    }
}
