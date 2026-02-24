/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.UnsupportedSelectorException;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.search.crossproject.CrossProjectIndexExpressionsRewriter;
import org.elasticsearch.search.crossproject.TargetProjects;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.invalidExclusion;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.invalidExpression;

public class IndexAbstractionResolver {
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexAbstractionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public ResolvedIndexExpressions resolveIndexAbstractions(
        final List<String> indices,
        final IndicesOptions indicesOptions,
        final ProjectMetadata projectMetadata,
        final Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        final BiPredicate<String, IndexComponentSelector> isAuthorized,
        final boolean includeDataStreams
    ) {
        final ResolvedIndexExpressions.Builder resolvedExpressionsBuilder = ResolvedIndexExpressions.builder();
        for (String originalIndexExpression : indices) {
            resolveIndexAbstraction(
                resolvedExpressionsBuilder,
                originalIndexExpression,
                originalIndexExpression, // in the case of local resolution, the local expression is always the same as the original
                indicesOptions,
                projectMetadata,
                allAuthorizedAndAvailableBySelector,
                isAuthorized,
                includeDataStreams,
                Set.of()
            );
        }
        return resolvedExpressionsBuilder.build();
    }

    public ResolvedIndexExpressions resolveIndexAbstractions(
        final List<String> indices,
        final IndicesOptions indicesOptions,
        final ProjectMetadata projectMetadata,
        final Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        final BiPredicate<String, IndexComponentSelector> isAuthorized,
        final TargetProjects targetProjects,
        final boolean includeDataStreams,
        @Nullable final String projectRouting
    ) {
        if (targetProjects == TargetProjects.LOCAL_ONLY_FOR_CPS_DISABLED) {
            final String message = "cannot resolve indices cross project if target set is local only";
            assert false : message;
            throw new IllegalArgumentException(message);
        }

        final String originProjectAlias = targetProjects.originProjectAlias();
        final Set<String> linkedProjectAliases = targetProjects.allProjectAliases();
        final ResolvedIndexExpressions.Builder resolvedExpressionsBuilder = ResolvedIndexExpressions.builder();
        for (String originalIndexExpression : indices) {
            final CrossProjectIndexExpressionsRewriter.IndexRewriteResult indexRewriteResult = CrossProjectIndexExpressionsRewriter
                .rewriteIndexExpression(originalIndexExpression, originProjectAlias, linkedProjectAliases, projectRouting);

            final String localIndexExpression = indexRewriteResult.localExpression();
            if (localIndexExpression == null) {
                // (there can be an exclusion without any local index expressions)
                // nothing to resolve locally so skip resolve abstraction call
                resolvedExpressionsBuilder.addRemoteExpressions(originalIndexExpression, indexRewriteResult.remoteExpressions());
                continue;
            }

            resolveIndexAbstraction(
                resolvedExpressionsBuilder,
                originalIndexExpression,
                localIndexExpression,
                indicesOptions,
                projectMetadata,
                allAuthorizedAndAvailableBySelector,
                isAuthorized,
                includeDataStreams,
                indexRewriteResult.remoteExpressions()
            );
        }
        return resolvedExpressionsBuilder.build();
    }

    private void resolveIndexAbstraction(
        final ResolvedIndexExpressions.Builder resolvedExpressionsBuilder,
        final String originalIndexExpression,
        final String localIndexExpression,
        final IndicesOptions indicesOptions,
        final ProjectMetadata projectMetadata,
        final Function<IndexComponentSelector, Set<String>> allAuthorizedAndAvailableBySelector,
        final BiPredicate<String, IndexComponentSelector> isAuthorized,
        final boolean includeDataStreams,
        final Set<String> remoteExpressions
    ) {
        if (localIndexExpression.isEmpty()) {
            throw invalidExpression();
        }

        String indexAbstraction;
        boolean minus = false;
        if (localIndexExpression.charAt(0) == '-') {
            indexAbstraction = localIndexExpression.substring(1);
            minus = true;
            if (indexAbstraction.isEmpty()) {
                throw invalidExclusion();
            }
        } else {
            indexAbstraction = localIndexExpression;
        }

        // Always check to see if there's a selector on the index expression
        final Tuple<String, String> expressionAndSelector = IndexNameExpressionResolver.splitSelectorExpression(indexAbstraction);
        final String selectorString = expressionAndSelector.v2();
        if (indicesOptions.allowSelectors() == false && selectorString != null) {
            throw new UnsupportedSelectorException(indexAbstraction);
        }
        indexAbstraction = expressionAndSelector.v1();
        IndexComponentSelector selector = IndexComponentSelector.getByKeyOrThrow(selectorString);

        // we always need to check for date math expressions
        indexAbstraction = IndexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);

        if (indicesOptions.expandWildcardExpressions() && Regex.isSimpleMatchPattern(indexAbstraction)) {
            final HashSet<String> resolvedIndices = new HashSet<>();
            for (String authorizedIndex : allAuthorizedAndAvailableBySelector.apply(selector)) {
                if (Regex.simpleMatch(indexAbstraction, authorizedIndex)
                    && isIndexVisibleUnderWildcardAccess(
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
                // Ignore empty result for exclusion expression
                if (minus == false) {
                    // es core honours allow_no_indices for each wildcard expression, we do the same here by throwing index not found.
                    if (indicesOptions.allowNoIndices() == false) {
                        throw new IndexNotFoundException(indexAbstraction);
                    }
                    resolvedExpressionsBuilder.addExpressions(originalIndexExpression, new HashSet<>(), SUCCESS, remoteExpressions);
                } else {
                    maybeAddWithRemoteExpressions(resolvedExpressionsBuilder, originalIndexExpression, remoteExpressions);
                }
            } else {
                if (minus) {
                    resolvedExpressionsBuilder.excludeFromLocalExpressions(resolvedIndices);
                    // Exclusion from local indices is done by excludeFromLocalExpressions.
                    // No need to add itself unless it has remote expressions.
                    maybeAddWithRemoteExpressions(resolvedExpressionsBuilder, originalIndexExpression, remoteExpressions);
                } else {
                    resolvedExpressionsBuilder.addExpressions(originalIndexExpression, resolvedIndices, SUCCESS, remoteExpressions);
                }
            }
        } else {
            final HashSet<String> resolvedIndices = new HashSet<>();
            resolveSelectorsAndCollect(indexAbstraction, selectorString, indicesOptions, resolvedIndices, projectMetadata);
            if (minus) {
                resolvedExpressionsBuilder.excludeFromLocalExpressions(resolvedIndices);
                maybeAddWithRemoteExpressions(resolvedExpressionsBuilder, originalIndexExpression, remoteExpressions);
            } else {
                final boolean authorized = isAuthorized.test(indexAbstraction, selector);
                if (authorized) {
                    boolean visible = indexExists(projectMetadata, indexAbstraction)
                        && isIndexVisibleUnderConcreteAccess(
                            indexAbstraction,
                            selectorString,
                            indicesOptions,
                            projectMetadata,
                            indexNameExpressionResolver,
                            includeDataStreams
                        );
                    final LocalIndexResolutionResult result = visible ? SUCCESS : CONCRETE_RESOURCE_NOT_VISIBLE;
                    resolvedExpressionsBuilder.addExpressions(originalIndexExpression, resolvedIndices, result, remoteExpressions);
                } else if (indicesOptions.ignoreUnavailable()) {
                    // ignoreUnavailable implies that the request should not fail if an index is not authorized
                    // so we map this expression to an empty list,
                    resolvedExpressionsBuilder.addExpressions(
                        originalIndexExpression,
                        new HashSet<>(),
                        CONCRETE_RESOURCE_UNAUTHORIZED,
                        remoteExpressions
                    );
                } else {
                    // store the calculated expansion as unauthorized, it will be rejected later
                    resolvedExpressionsBuilder.addExpressions(
                        originalIndexExpression,
                        resolvedIndices,
                        CONCRETE_RESOURCE_UNAUTHORIZED,
                        remoteExpressions
                    );
                }
            }
        }
    }

    private static void maybeAddWithRemoteExpressions(
        ResolvedIndexExpressions.Builder resolvedExpressionsBuilder,
        String originalIndexExpression,
        Set<String> remoteExpressions
    ) {
        if (false == remoteExpressions.isEmpty()) {
            resolvedExpressionsBuilder.addRemoteExpressions(originalIndexExpression, remoteExpressions);
        }
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

    public static boolean isIndexVisibleUnderConcreteAccess(
        String expression,
        @Nullable String selectorString,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver resolver,
        boolean includeDataStreams
    ) {
        assert Regex.isSimpleMatchPattern(expression) == false : "Expected a concrete expression";
        return isIndexVisible(expression, selectorString, expression, indicesOptions, projectMetadata, resolver, includeDataStreams, false);
    }

    public static boolean isIndexVisibleUnderWildcardAccess(
        String expression,
        @Nullable String selectorString,
        String index,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver resolver,
        boolean includeDataStreams
    ) {
        assert Regex.isSimpleMatchPattern(expression) : "Expected a wildcard expression";
        return isIndexVisible(expression, selectorString, index, indicesOptions, projectMetadata, resolver, includeDataStreams, true);
    }

    private static boolean isIndexVisible(
        String expression,
        @Nullable String selectorString,
        String index,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver resolver,
        boolean includeDataStreams,
        boolean isWildcardExpression
    ) {
        IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(index);
        if (indexAbstraction == null) {
            throw new IllegalStateException("could not resolve index abstraction [" + index + "]");
        }
        if (indexAbstraction.getType() == IndexAbstraction.Type.VIEW) {
            return indicesOptions.wildcardOptions().resolveViews();
        }
        final boolean isHidden = indexAbstraction.isHidden();
        boolean isVisible = isWildcardExpression == false
            || isHidden == false
            || indicesOptions.expandWildcardsHidden()
            || isVisibleDueToImplicitHidden(expression, index);
        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
            // it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
            // it's complicated to support those options with aliases pointing to multiple indices...
            if (indicesOptions.ignoreAliases()) {
                return false;
            }

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
                            includeDataStreams,
                            isWildcardExpression
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

        IndexMetadata indexMetadata = projectMetadata.index(indexAbstraction.getIndices().getFirst());

        if (isWildcardExpression == false) {
            if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.forbidClosedIndices()) {
                return false;
            }

            // see IndexNameExpressionResolver#addIndex for reference
            if (indexMetadata.getSettings().getAsBoolean("index.frozen", false) && indicesOptions.ignoreThrottled()) {
                return false;
            }

            return true;
        }

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
        assert Regex.isSimpleMatchPattern(expression) : "Expected wildcard expression";
        return index.startsWith(".") && expression.startsWith(".");
    }

    private static boolean indexExists(ProjectMetadata projectMetadata, String indexAbstraction) {
        return projectMetadata.getIndicesLookup().get(indexAbstraction) != null;
    }
}
