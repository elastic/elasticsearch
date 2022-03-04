/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexAbstractionResolver {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexAbstractionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public List<String> resolveIndexAbstractions(
        String[] indices,
        IndicesOptions indicesOptions,
        Metadata metadata,
        boolean includeDataStreams
    ) {
        return resolveIndexAbstractions(Arrays.asList(indices), indicesOptions, metadata, includeDataStreams);
    }

    public List<String> resolveIndexAbstractions(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        Metadata metadata,
        boolean includeDataStreams
    ) {
        final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();
        Set<String> availableIndexAbstractions = metadata.getIndicesLookup().keySet();
        return resolveIndexAbstractions(
            indices,
            indicesOptions,
            metadata,
            availableIndexAbstractions,
            replaceWildcards,
            includeDataStreams
        );
    }

    public List<String> resolveIndexAbstractions(
        Iterable<String> indices,
        IndicesOptions indicesOptions,
        Metadata metadata,
        Collection<String> availableIndexAbstractions,
        boolean replaceWildcards,
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

            // we always need to check for date math expressions
            final String dateMathName = IndexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);
            if (dateMathName != indexAbstraction) {
                assert dateMathName.equals(indexAbstraction) == false;
                if (replaceWildcards && Regex.isSimpleMatchPattern(dateMathName)) {
                    // continue
                    indexAbstraction = dateMathName;
                } else if (availableIndexAbstractions.contains(dateMathName)
                    && isIndexVisible(
                        indexAbstraction,
                        dateMathName,
                        indicesOptions,
                        metadata,
                        indexNameExpressionResolver,
                        includeDataStreams,
                        true
                    )) {
                        if (minus) {
                            finalIndices.remove(dateMathName);
                        } else {
                            finalIndices.add(dateMathName);
                        }
                    } else {
                        if (indicesOptions.ignoreUnavailable() == false) {
                            throw new IndexNotFoundException(dateMathName);
                        }
                    }
            }

            if (replaceWildcards && Regex.isSimpleMatchPattern(indexAbstraction)) {
                wildcardSeen = true;
                Set<String> resolvedIndices = new HashSet<>();
                for (String authorizedIndex : availableIndexAbstractions) {
                    if (Regex.simpleMatch(indexAbstraction, authorizedIndex)
                        && isIndexVisible(
                            indexAbstraction,
                            authorizedIndex,
                            indicesOptions,
                            metadata,
                            indexNameExpressionResolver,
                            includeDataStreams
                        )) {
                        resolvedIndices.add(authorizedIndex);
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
            } else if (dateMathName.equals(indexAbstraction)) {
                if (minus) {
                    finalIndices.remove(indexAbstraction);
                } else if (indicesOptions.ignoreUnavailable() == false || availableIndexAbstractions.contains(indexAbstraction)) {
                    finalIndices.add(indexAbstraction);
                }
            }
        }
        return finalIndices;
    }

    public static boolean isIndexVisible(
        String expression,
        String index,
        IndicesOptions indicesOptions,
        Metadata metadata,
        IndexNameExpressionResolver resolver,
        boolean includeDataStreams
    ) {
        return isIndexVisible(expression, index, indicesOptions, metadata, resolver, includeDataStreams, false);
    }

    public static boolean isIndexVisible(
        String expression,
        String index,
        IndicesOptions indicesOptions,
        Metadata metadata,
        IndexNameExpressionResolver resolver,
        boolean includeDataStreams,
        boolean dateMathExpression
    ) {
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
        if (indexAbstraction == null) {
            throw new IllegalStateException("could not resolve index abstraction [" + index + "]");
        }
        final boolean isHidden = indexAbstraction.isHidden();
        boolean isVisible = isHidden == false || indicesOptions.expandWildcardsHidden() || isVisibleDueToImplicitHidden(expression, index);
        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
            // it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
            // complicated to support those options with aliases pointing to multiple indices...
            return isVisible && indicesOptions.ignoreAliases() == false;
        }
        if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
            if (includeDataStreams == false) {
                return false;
            }
            if (indexAbstraction.isSystem()) {
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
            } else {
                return isVisible;
            }
        }
        assert indexAbstraction.getIndices().size() == 1 : "concrete index must point to a single index";
        // since it is a date math expression, we consider the index visible regardless of open/closed/hidden as the user is using
        // date math to explicitly reference the index
        if (dateMathExpression) {
            assert IndexMetadata.State.values().length == 2 : "a new IndexMetadata.State value may need to be handled!";
            return true;
        }
        if (isVisible == false) {
            return false;
        }
        if (indexAbstraction.isSystem()) {
            // check if it is net new
            if (resolver.getNetNewSystemIndexPredicate().test(indexAbstraction.getName())) {
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

            // does the system index back a system data stream?
            if (indexAbstraction.getParentDataStream() != null) {
                if (indexAbstraction.getParentDataStream().isSystem() == false) {
                    assert false : "system index is part of a data stream that is not a system data stream";
                    throw new IllegalStateException("system index is part of a data stream that is not a system data stream");
                }
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
        }

        IndexMetadata indexMetadata = metadata.index(indexAbstraction.getIndices().get(0));
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.OPEN && indicesOptions.expandWildcardsOpen()) {
            return true;
        }
        return false;
    }

    private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
        return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
    }
}
