/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class InMemoryViewResolver extends ViewResolver {
    protected Supplier<ViewMetadata> metadata;
    protected LinkedHashSet<String> indices = new LinkedHashSet<>();

    public InMemoryViewResolver(ClusterService clusterService, Supplier<ViewMetadata> metadata) {
        super(clusterService, null);
        this.metadata = metadata;
    }

    @Override
    protected ViewMetadata getMetadata() {
        return metadata.get();
    }

    @Override
    protected Map<String, IndexAbstraction> getIndicesLookup() {
        Map<String, IndexAbstraction> viewsLookup = getMetadata().views()
            .values()
            .stream()
            .collect(Collectors.toMap(IndexAbstraction::getName, v -> v));
        for (String index : indices) {
            viewsLookup.put(index, null);
        }
        return viewsLookup;
    }

    protected boolean viewsFeatureEnabled() {
        // This is a test implementation, so we assume the feature is always enabled
        return true;
    }

    public void addIndex(String name) {
        indices.add(name);
    }

    public void clear() {
        indices.clear();
    }
}
