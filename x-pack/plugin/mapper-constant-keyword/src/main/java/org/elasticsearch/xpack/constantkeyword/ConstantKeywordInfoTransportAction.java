/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class ConstantKeywordInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final XPackLicenseState licenseState;

    @Inject
    public ConstantKeywordInfoTransportAction(TransportService transportService, ActionFilters actionFilters,
                                        Settings settings, XPackLicenseState licenseState) {
        super(XPackInfoFeatureAction.CONSTANT_KEYWORD.name(), transportService, actionFilters);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.CONSTANT_KEYWORD;
    }

    @Override
    public boolean available() {
        return licenseState.isConstantKeywordAllowed();
    }

    @Override
    public boolean enabled() {
        return true;
    }

}
