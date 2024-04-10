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

public class ResolvedIndices {
    @Nullable
    private final SearchContextId searchContextId;
    private final Map<String, OriginalIndices> remoteClusterIndices;
    private final OriginalIndices localIndices;
    private final Map<Index, IndexMetadata> localIndexMetadata;

    private ResolvedIndices(
        Map<String, OriginalIndices> remoteClusterIndices,
        OriginalIndices localIndices,
        Map<Index, IndexMetadata> localIndexMetadata,
        @Nullable SearchContextId searchContextId
    ) {
        this.remoteClusterIndices = Collections.unmodifiableMap(remoteClusterIndices);
        this.localIndices = localIndices;
        this.localIndexMetadata = Collections.unmodifiableMap(localIndexMetadata);
        this.searchContextId = searchContextId;
    }

    private ResolvedIndices(
        Map<String, OriginalIndices> remoteClusterIndices,
        OriginalIndices localIndices,
        Map<Index, IndexMetadata> localIndexMetadata
    ) {
        this(remoteClusterIndices, localIndices, localIndexMetadata, null);
    }

    public Map<String, OriginalIndices> getRemoteClusterIndices() {
        return remoteClusterIndices;
    }

    public OriginalIndices getLocalIndices() {
        return localIndices;
    }

    public Map<Index, IndexMetadata> getLocalIndexMetadata() {
        return localIndexMetadata;
    }

    public Index[] getConcreteLocalIndices() {
        return localIndexMetadata.keySet().toArray(Index[]::new);
    }

    @Nullable
    public SearchContextId getSearchContextId() {
        return searchContextId;
    }

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
