/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Container for information about results of the resolution of index expression.
 * Contains local indices, map of remote indices and metadata.
 */
public class ResolvedIndices {
    @Nullable
    private final SearchContextId searchContextId;
    private final Map<String, OriginalIndices> remoteClusterIndices;
    @Nullable
    private final OriginalIndices localIndices;
    private final Map<Index, IndexMetadata> localIndexMetadata;

    ResolvedIndices(
        Map<String, OriginalIndices> remoteClusterIndices,
        @Nullable OriginalIndices localIndices,
        Map<Index, IndexMetadata> localIndexMetadata,
        @Nullable SearchContextId searchContextId
    ) {
        this.remoteClusterIndices = Collections.unmodifiableMap(remoteClusterIndices);
        this.localIndices = localIndices;
        this.localIndexMetadata = Collections.unmodifiableMap(localIndexMetadata);
        this.searchContextId = searchContextId;
    }

    ResolvedIndices(
        Map<String, OriginalIndices> remoteClusterIndices,
        @Nullable OriginalIndices localIndices,
        Map<Index, IndexMetadata> localIndexMetadata
    ) {
        this(remoteClusterIndices, localIndices, localIndexMetadata, null);
    }

    /**
     * Get the remote cluster indices, structured as a map where the key is the remote cluster alias.
     * <br/>
     * <br/>
     * NOTE: The returned indices are *not* guaranteed to be concrete indices that exist.
     * In addition to simple concrete index names, returned index names can be any combination of the following:
     * <ul>
     *     <li>Aliases</li>
     *     <li>Wildcards</li>
     *     <li>Invalid index/alias names</li>
     * </ul>
     *
     * @return The remote cluster indices map
     */
    public Map<String, OriginalIndices> getRemoteClusterIndices() {
        return remoteClusterIndices;
    }

    /**
     * Get the local cluster indices.
     * If the returned value is null, no local cluster indices are referenced.
     * If the returned value is an {@link OriginalIndices} instance with an empty or null {@link OriginalIndices#indices()} array,
     * potentially all local cluster indices are referenced, depending on if {@link OriginalIndices#indicesOptions()} is configured to
     * expand wildcards.
     * <br/>
     * <br/>
     * NOTE: The returned indices are *not* guaranteed to be concrete indices that exist.
     * In addition to simple concrete index names, returned index names can be any combination of the following:
     * <ul>
     *     <li>Aliases</li>
     *     <li>Wildcards</li>
     *     <li>Invalid index/alias names</li>
     * </ul>
     *
     * @return The local cluster indices
     */
    @Nullable
    public OriginalIndices getLocalIndices() {
        return localIndices;
    }

    /**
     * Get metadata for concrete local cluster indices.
     * All indices returned are guaranteed to be concrete indices that exist.
     *
     * @return Metadata for concrete local cluster indices
     */
    public Map<Index, IndexMetadata> getConcreteLocalIndicesMetadata() {
        return localIndexMetadata;
    }

    /**
     * Get the concrete local cluster indices.
     * All indices returned are guaranteed to be concrete indices that exist.
     *
     * @return The concrete local cluster indices
     */
    public Index[] getConcreteLocalIndices() {
        return localIndexMetadata.keySet().toArray(Index[]::new);
    }

    /**
     * Get the search context ID.
     * Returns a non-null value only when the instance is created using
     * {@link ResolvedIndices#resolveWithPIT(PointInTimeBuilder, IndicesOptions, ProjectMetadata, NamedWriteableRegistry)}.
     *
     * @return The search context ID
     */
    @Nullable
    public SearchContextId getSearchContextId() {
        return searchContextId;
    }

    /**
     * Create a new {@link ResolvedIndices} instance from an {@link IndicesRequest}.
     *
     * @param request The indices request
     * @param projectMetadata The project holding the indices
     * @param indexNameExpressionResolver The index name expression resolver used to resolve concrete local indices
     * @param remoteClusterService The remote cluster service used to group remote cluster indices
     * @param startTimeInMillis The request start time in milliseconds
     * @return a new {@link ResolvedIndices} instance
     */
    public static ResolvedIndices resolveWithIndicesRequest(
        IndicesRequest request,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        long startTimeInMillis
    ) {
        return resolveWithIndexNamesAndOptions(
            request.indices(),
            request.indicesOptions(),
            projectMetadata,
            indexNameExpressionResolver,
            remoteClusterService,
            startTimeInMillis
        );
    }

    public static ResolvedIndices resolveWithIndexNamesAndOptions(
        String[] indexNames,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        long startTimeInMillis
    ) {
        final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(indicesOptions, indexNames);

        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

        Index[] concreteLocalIndices = localIndices == null
            ? Index.EMPTY_ARRAY
            : indexNameExpressionResolver.concreteIndices(projectMetadata, localIndices, startTimeInMillis);

        // prevent using selectors with remote cluster patterns
        for (final var indicesPerRemoteClusterAlias : remoteClusterIndices.entrySet()) {
            final String[] indices = indicesPerRemoteClusterAlias.getValue().indices();
            if (indices != null) {
                for (final String index : indices) {
                    if (IndexNameExpressionResolver.hasSelectorSuffix(index)) {
                        throw new InvalidIndexNameException(index, "Selectors are not yet supported on remote cluster patterns");
                    }
                }
            }
        }

        return new ResolvedIndices(
            remoteClusterIndices,
            localIndices,
            resolveLocalIndexMetadata(concreteLocalIndices, projectMetadata, true)
        );
    }

    /**
     * Create a new {@link ResolvedIndices} instance from a {@link PointInTimeBuilder}.
     *
     * @param pit The point-in-time builder
     * @param indicesOptions The indices options to propagate to the new {@link ResolvedIndices} instance
     * @param projectMetadata The project holding the indices
     * @param namedWriteableRegistry The named writeable registry used to decode the search context ID
     * @return a new {@link ResolvedIndices} instance
     */
    public static ResolvedIndices resolveWithPIT(
        PointInTimeBuilder pit,
        IndicesOptions indicesOptions,
        ProjectMetadata projectMetadata,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        final SearchContextId searchContextId = pit.getSearchContextId(namedWriteableRegistry);
        final Map<String, Set<Index>> indicesFromSearchContext = new HashMap<>();
        for (var entry : searchContextId.shards().entrySet()) {
            String clusterAlias = entry.getValue().getClusterAlias();
            if (clusterAlias == null) {
                clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            }

            indicesFromSearchContext.computeIfAbsent(clusterAlias, s -> new HashSet<>()).add(entry.getKey().getIndex());
        }

        OriginalIndices localIndices;
        Index[] concreteLocalIndices;
        Set<Index> localIndicesSet = indicesFromSearchContext.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        if (localIndicesSet != null) {
            concreteLocalIndices = localIndicesSet.toArray(Index[]::new);
            localIndices = new OriginalIndices(localIndicesSet.stream().map(Index::getName).toArray(String[]::new), indicesOptions);
        } else {
            concreteLocalIndices = Index.EMPTY_ARRAY;
            // Set localIndices to null because a non-null value with a null or 0-length indices array will be resolved to all indices by
            // IndexNameExpressionResolver
            localIndices = null;
        }

        Map<String, OriginalIndices> remoteClusterIndices = new HashMap<>();
        for (var entry : indicesFromSearchContext.entrySet()) {
            OriginalIndices originalIndices = new OriginalIndices(
                entry.getValue().stream().map(Index::getName).toArray(String[]::new),
                indicesOptions
            );
            remoteClusterIndices.put(entry.getKey(), originalIndices);
        }

        // Don't fail on missing indices to handle point-in-time requests that reference deleted indices
        return new ResolvedIndices(
            remoteClusterIndices,
            localIndices,
            resolveLocalIndexMetadata(concreteLocalIndices, projectMetadata, false),
            searchContextId
        );
    }

    /**
     * Create a new {@link ResolvedIndices} instance from a Map of Projects to {@link ResolvedIndexExpressions}. This is intended to be
     * used for Cross-Project Search (CPS).
     *
     * @param localIndices this value is set as-is in the resulting ResolvedIndices.
     * @param localIndexMetadata this value is set as-is in the resulting ResolvedIndices.
     * @param remoteExpressions the map of project names to {@link ResolvedIndexExpressions}. This map is used to create the
     *                          {@link ResolvedIndices#getRemoteClusterIndices()} for the resulting ResolvedIndices. Each project keyed
     *                          in the map is guaranteed to have at least one index for the index expression provided by the user.
     *                          The resulting {@link ResolvedIndices#getRemoteClusterIndices()} will map to the original index expression
     *                          provided by the user. For example, if the user requested "logs" and "project-1" resolved that to "logs-1",
     *                          then the result will map "project-1" to "logs". We rely on the remote search request to expand "logs" back
     *                          to "logs-1".
     * @param indicesOptions this value is set as-is in the resulting ResolvedIndices.
     */
    public static ResolvedIndices resolveWithIndexExpressions(
        OriginalIndices localIndices,
        Map<Index, IndexMetadata> localIndexMetadata,
        Map<String, ResolvedIndexExpressions> remoteExpressions,
        IndicesOptions indicesOptions
    ) {
        Map<String, OriginalIndices> remoteIndices = remoteExpressions.entrySet().stream().collect(HashMap::new, (map, entry) -> {
            var indices = entry.getValue().expressions().stream().filter(expression -> {
                var resolvedExpressions = expression.localExpressions();
                var successfulResolution = resolvedExpressions
                    .localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
                // if the expression is a wildcard, it will be successful even if there are no indices, so filter for no indices
                var hasResolvedIndices = resolvedExpressions.indices().isEmpty() == false;
                return successfulResolution && hasResolvedIndices;
            }).map(ResolvedIndexExpression::original).toArray(String[]::new);
            if (indices.length > 0) {
                map.put(entry.getKey(), new OriginalIndices(indices, indicesOptions));
            }
        }, Map::putAll);

        return new ResolvedIndices(remoteIndices, localIndices, localIndexMetadata);
    }

    private static Map<Index, IndexMetadata> resolveLocalIndexMetadata(
        Index[] concreteLocalIndices,
        ProjectMetadata projectMetadata,
        boolean failOnMissingIndex
    ) {
        Map<Index, IndexMetadata> localIndexMetadata = new HashMap<>();
        for (Index index : concreteLocalIndices) {
            IndexMetadata indexMetadata = projectMetadata.index(index);
            if (indexMetadata == null) {
                if (failOnMissingIndex) {
                    throw new IndexNotFoundException(index);
                }
                continue;
            }

            localIndexMetadata.put(index, indexMetadata);
        }

        return localIndexMetadata;
    }
}
