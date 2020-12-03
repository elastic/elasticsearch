/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;

public class OperatorPrivilegesInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final XPackLicenseState licenseState;
    private final boolean enabled;

    @Inject
    public OperatorPrivilegesInfoTransportAction(TransportService transportService, ActionFilters actionFilters,
                                                 Settings settings, XPackLicenseState licenseState) {
        super(XPackInfoFeatureAction.OPERATOR_PRIVILEGES.name(), transportService, actionFilters);
        this.licenseState = licenseState;
        enabled = OPERATOR_PRIVILEGES_ENABLED.get(settings);
    }

    @Override
    protected String name() {
        return XPackField.OPERATOR_PRIVILEGES;
    }

    @Override
    protected boolean available() {
        return licenseState.isAllowed(XPackLicenseState.Feature.OPERATOR_PRIVILEGES);
    }

    @Override
    protected boolean enabled() {
        return enabled;
    }
}
