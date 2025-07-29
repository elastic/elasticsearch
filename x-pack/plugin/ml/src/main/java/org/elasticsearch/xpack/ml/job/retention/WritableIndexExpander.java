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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class is used to expand index patterns and filter out read-only indices.
 * It is used in the context of machine learning jobs retention to ensure that only writable indices are considered.
 */
public class WritableIndexExpander {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private static WritableIndexExpander INSTANCE;

    public static void initialize(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        INSTANCE = new WritableIndexExpander(clusterService, indexNameExpressionResolver);
    }

    public static void initialize(WritableIndexExpander newInstance) {
        INSTANCE = newInstance;
    }

    public static WritableIndexExpander getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("WritableIndexExpander is not initialized");
        }
        return INSTANCE;
    }

    protected WritableIndexExpander(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
    }

    public ArrayList<String> getWritableIndices(String indexPattern) {
        return getWritableIndices(List.of(indexPattern));
    }

    public ArrayList<String> getWritableIndices(Collection<String> indices) {
        if (indices == null || indices.isEmpty()) {
            return new ArrayList<>();
        }
        var clusterState = clusterService.state();
        return indices.stream()
            .map(index -> indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN, index))
            .flatMap(Arrays::stream)
            .filter(index -> (isIndexReadOnly(index) == false))
            .collect(Collectors.toCollection(ArrayList::new));
    }

    private Boolean isIndexReadOnly(String indexName) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Failed to identify if index is read-only: index [" + indexName + "] not found");
        }
        if (indexMetadata.getSettings() == null) {
            throw new IllegalStateException("Settings for index [" + indexName + "] are unavailable");
        }
        return IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings());
    }
}
