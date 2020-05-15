/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.slm.SLMFeatureSetUsage;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.util.Map;

public class SLMFeatureSet implements XPackFeatureSet {

    private final XPackLicenseState licenseState;
    private ClusterService clusterService;

    @Inject
    public SLMFeatureSet(@Nullable XPackLicenseState licenseState, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.SNAPSHOT_LIFECYCLE;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isAllowed(XPackLicenseState.Feature.ILM);
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
        boolean available = licenseState.isAllowed(XPackLicenseState.Feature.ILM);
        final SnapshotLifecycleMetadata slmMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        final SLMFeatureSetUsage usage = new SLMFeatureSetUsage(available, slmMeta == null ? null : slmMeta.getStats());
        listener.onResponse(usage);
    }

}
