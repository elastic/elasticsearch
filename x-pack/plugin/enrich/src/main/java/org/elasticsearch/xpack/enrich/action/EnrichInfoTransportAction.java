/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

public class EnrichInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final XPackLicenseState licenseState;

    @Inject
    public EnrichInfoTransportAction(TransportService transportService, ActionFilters actionFilters, XPackLicenseState licenseState) {
        super(XPackInfoFeatureAction.ENRICH.name(), transportService, actionFilters);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.ENRICH;
    }

    @Override
    public boolean available() {
        return licenseState.isAllowed(XPackLicenseState.Feature.ENRICH);
    }

    @Override
    public boolean enabled() {
        return true;
    }

}
