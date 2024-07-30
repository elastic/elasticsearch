/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
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
     * {@link ResolvedIndices#resolveWithPIT(PointInTimeBuilder, IndicesOptions, ClusterState, NamedWriteableRegistry)}.
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
     * @param clusterState The cluster state
     * @param indexNameExpressionResolver The index name expression resolver used to resolve concrete local indices
     * @param remoteClusterService The remote cluster service used to group remote cluster indices
     * @param startTimeInMillis The request start time in milliseconds
     * @return a new {@link ResolvedIndices} instance
     */
    public static ResolvedIndices resolveWithIndicesRequest(
        IndicesRequest request,
        ClusterState clusterState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        long startTimeInMillis
    ) {
        final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(
            request.indicesOptions(),
            request.indices()
        );
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

        Index[] concreteLocalIndices = localIndices == null
            ? Index.EMPTY_ARRAY
            : indexNameExpressionResolver.concreteIndices(clusterState, localIndices, startTimeInMillis);

        return new ResolvedIndices(remoteClusterIndices, localIndices, resolveLocalIndexMetadata(concreteLocalIndices, clusterState, true));
    }

    /**
     * Create a new {@link ResolvedIndices} instance from a {@link PointInTimeBuilder}.
     *
     * @param pit The point-in-time builder
     * @param indicesOptions The indices options to propagate to the new {@link ResolvedIndices} instance
     * @param clusterState The cluster state
     * @param namedWriteableRegistry The named writeable registry used to decode the search context ID
     * @return a new {@link ResolvedIndices} instance
     */
    public static ResolvedIndices resolveWithPIT(
        PointInTimeBuilder pit,
        IndicesOptions indicesOptions,
        ClusterState clusterState,
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
            resolveLocalIndexMetadata(concreteLocalIndices, clusterState, false),
            searchContextId
        );
    }

    private static Map<Index, IndexMetadata> resolveLocalIndexMetadata(
        Index[] concreteLocalIndices,
        ClusterState clusterState,
        boolean failOnMissingIndex
    ) {
        Map<Index, IndexMetadata> localIndexMetadata = new HashMap<>();
        for (Index index : concreteLocalIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
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
