/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.utils.LicenseUtils;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class EnterpriseSearchInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final boolean enabled;

    private final XPackLicenseState licenseState;

    @Inject
    public EnterpriseSearchInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        XPackLicenseState licenseState
    ) {
        super(XPackInfoFeatureAction.ENTERPRISE_SEARCH.name(), transportService, actionFilters);
        this.enabled = XPackSettings.ENTERPRISE_SEARCH_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.ENTERPRISE_SEARCH;
    }

    @Override
    public boolean available() {
        return LicenseUtils.PLATINUM_LICENSED_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }
}
