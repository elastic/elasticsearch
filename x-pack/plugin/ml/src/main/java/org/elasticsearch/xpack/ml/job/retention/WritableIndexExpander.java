/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class WritableIndexExpander {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public WritableIndexExpander(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
    }

    protected ArrayList<String> getWritableIndices(String indexPattern) {
        var clusterState = clusterService.state();
        var concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN,
            indexPattern
        );
        return Arrays.stream(concreteIndices).filter(index -> isIndexReadOnly(index) == false).collect(Collectors.toCollection(ArrayList::new));
    }

    protected ArrayList<String> getWritableIndices(Collection<String> indices) {
        return indices.stream()
            .filter(index -> isIndexReadOnly(index) == false)
            .collect(Collectors.toCollection(ArrayList::new));
    }

    private Boolean isIndexReadOnly(String indexName) {
        return IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(clusterService.state().metadata().getProject().index(indexName).getSettings());
    }
}
