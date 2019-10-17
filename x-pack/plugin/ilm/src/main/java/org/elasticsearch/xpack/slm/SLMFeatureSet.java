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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.slm.SLMFeatureSetUsage;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.util.Map;

public class SLMFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private ClusterService clusterService;

    @Inject
    public SLMFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.enabled = XPackSettings.SNAPSHOT_LIFECYCLE_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.SNAPSHOT_LIFECYCLE;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isIndexLifecycleAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<Usage> listener) {
        final ClusterState state = clusterService.state();
        boolean available = licenseState.isIndexLifecycleAllowed();
        final SnapshotLifecycleMetadata slmMeta = state.metaData().custom(SnapshotLifecycleMetadata.TYPE);
        final SLMFeatureSetUsage usage = new SLMFeatureSetUsage(available, enabled,
            slmMeta == null ? null : slmMeta.getStats());
        listener.onResponse(usage);
    }

}
