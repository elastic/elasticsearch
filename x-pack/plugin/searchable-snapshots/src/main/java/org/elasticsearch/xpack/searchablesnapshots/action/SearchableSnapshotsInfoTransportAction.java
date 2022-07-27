/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

public class SearchableSnapshotsInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final XPackLicenseState licenseState;

    @Inject
    public SearchableSnapshotsInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState
    ) {
        super(XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS.name(), transportService, actionFilters);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.SEARCHABLE_SNAPSHOTS;
    }

    @Override
    public boolean available() {
        return SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    public boolean enabled() {
        return true;
    }
}
