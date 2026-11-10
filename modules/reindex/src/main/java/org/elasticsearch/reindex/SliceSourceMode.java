/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Arrays;

/**
 * Reading a slice-enabled source without a {@code _slice} reads every slice, and the documents written back keep the slice they were
 * read from, exactly as when the source is read with a {@code _slice}. Both are the same thing to the actions that write the documents,
 * so a source without a {@code _slice} is marked as read in slice mode, with unrestricted routing, as soon as it targets a slice-enabled
 * index.
 */
final class SliceSourceMode {

    private SliceSourceMode() {}

    /**
     * Marks {@code source} as read in slice mode when it has no {@code _slice} and targets a slice-enabled index. Remote index names are
     * skipped, they cannot be resolved against the local cluster state.
     */
    static void markIfSliceEnabled(SearchRequest source, IndexNameExpressionResolver indexResolver, ProjectMetadata projectMetadata) {
        if (source.isRoutingFromSlice() || source.routing() != null) {
            return;
        }
        final SearchRequest localSource = skipRemoteIndexNames(source);
        if (localSource.indices().length == 0) {
            return;
        }
        for (Index index : indexResolver.concreteIndices(projectMetadata, localSource)) {
            final IndexMetadata indexMetadata = projectMetadata.index(index);
            if (indexMetadata != null && IndexSettings.SLICE_ENABLED.get(indexMetadata.getSettings())) {
                source.setRoutingFromSlice(true);
                return;
            }
        }
    }

    static SearchRequest skipRemoteIndexNames(SearchRequest source) {
        IndicesOptions indicesOptions = source.indicesOptions();
        if (indicesOptions.resolveCrossProjectIndexExpression()) {
            indicesOptions = CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout(indicesOptions);
        }
        // An index expression that references a remote cluster uses ":" to separate the cluster-alias from the index portion of the
        // expression, e.g., cluster0:index-name
        return new SearchRequest(source).indicesOptions(indicesOptions)
            .indices(
                Arrays.stream(source.indices()).filter(name -> RemoteClusterAware.isRemoteIndexName(name) == false).toArray(String[]::new)
            );
    }
}
