/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

import static org.elasticsearch.xpack.lucene.bwc.OldLuceneVersions.ARCHIVE_FEATURE;

public class ArchiveInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final XPackLicenseState licenseState;

    @Inject
    public ArchiveInfoTransportAction(TransportService transportService, ActionFilters actionFilters, XPackLicenseState licenseState) {
        super(XPackInfoFeatureAction.ARCHIVE.name(), transportService, actionFilters);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.ARCHIVE;
    }

    @Override
    public boolean available() {
        return ARCHIVE_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    public boolean enabled() {
        return true;
    }
}
