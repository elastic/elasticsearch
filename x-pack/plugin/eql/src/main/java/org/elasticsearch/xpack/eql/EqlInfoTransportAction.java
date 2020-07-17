/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;

public class EqlInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public EqlInfoTransportAction(TransportService transportService, ActionFilters actionFilters,
                                  Settings settings, XPackLicenseState licenseState) {
        super(XPackInfoFeatureAction.EQL.name(), transportService, actionFilters);
        this.enabled = EqlPlugin.isEnabled(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.EQL;
    }

    @Override
    public boolean available() {
        return licenseState.isAllowed(XPackLicenseState.Feature.EQL);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

}
