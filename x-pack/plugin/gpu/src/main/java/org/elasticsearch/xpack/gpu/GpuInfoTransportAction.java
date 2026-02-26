/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

/**
 * Handles the {@code _xpack} info endpoint for the GPU vector indexing feature.
 * Reports {@code available} based on the Enterprise license check.
 * Reports {@code enabled} based on local node's GPU support and setting.
 * Detailed per-node GPU availability and usage statistics are reported
 * via the {@code _xpack/usage} endpoint.
 */
public class GpuInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final XPackLicenseState licenseState;
    private final boolean localNodeGpuEnabled;

    @Inject
    public GpuInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        Settings settings
    ) {
        super(XPackInfoFeatureAction.GPU_VECTOR_INDEXING.name(), transportService, actionFilters);
        this.licenseState = licenseState;
        GPUPlugin.GpuMode gpuMode = GPUPlugin.VECTORS_INDEXING_USE_GPU_NODE_SETTING.get(settings);
        this.localNodeGpuEnabled = gpuMode != GPUPlugin.GpuMode.FALSE && GPUSupport.isSupported();
    }

    @Override
    protected String name() {
        return XPackField.GPU_VECTOR_INDEXING;
    }

    @Override
    protected boolean available() {
        return GPUPlugin.GPU_INDEXING_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    protected boolean enabled() {
        return localNodeGpuEnabled;
    }
}
