/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.slm.SLMFeatureSetUsage;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.util.Map;

public class SLMFeatureSet implements XPackFeatureSet {

    private ClusterService clusterService;

    @Inject
    public SLMFeatureSet(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.SNAPSHOT_LIFECYCLE;
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
        final ClusterState state = clusterService.state();
        final SnapshotLifecycleMetadata slmMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        final SLMFeatureSetUsage usage = new SLMFeatureSetUsage(slmMeta == null ? null : slmMeta.getStats());
        listener.onResponse(usage);
    }

}
