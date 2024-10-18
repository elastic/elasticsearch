/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class ProfilingInfoTransportAction extends XPackInfoFeatureTransportAction {
    private final boolean enabled;
    private final ProfilingLicenseChecker licenseChecker;

    @Inject
    public ProfilingInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        ProfilingLicenseChecker licenseChecker
    ) {
        super(XPackInfoFeatureAction.UNIVERSAL_PROFILING.name(), transportService, actionFilters);
        this.enabled = XPackSettings.PROFILING_ENABLED.get(settings);
        this.licenseChecker = licenseChecker;
    }

    @Override
    public String name() {
        return XPackField.UNIVERSAL_PROFILING;
    }

    @Override
    public boolean available() {
        return licenseChecker.isSupportedLicense();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }
}
