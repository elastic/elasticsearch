/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.frozen.FrozenIndicesFeatureSetUsage;

import java.util.Map;

public class FrozenIndicesFeatureSet implements XPackFeatureSet {

    private final ClusterService clusterService;

    @Inject
    public FrozenIndicesFeatureSet(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.FROZEN_INDICES;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<Usage> listener) {
        int numFrozenIndices = 0;
        for (IndexMetadata indexMetadata : clusterService.state().metadata()) {
            if (FrozenEngine.INDEX_FROZEN.get(indexMetadata.getSettings())) {
                numFrozenIndices++;
            }
        }
        listener.onResponse(new FrozenIndicesFeatureSetUsage(true, true, numFrozenIndices));
    }
}
