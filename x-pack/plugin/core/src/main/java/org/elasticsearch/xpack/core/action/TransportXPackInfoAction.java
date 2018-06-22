/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackInfoResponse;
import org.elasticsearch.license.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.license.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackBuild;
import org.elasticsearch.xpack.core.XPackFeatureSet;

import java.util.Set;
import java.util.stream.Collectors;

public class TransportXPackInfoAction extends HandledTransportAction<XPackInfoRequest, XPackInfoResponse> {

    private final LicenseService licenseService;
    private final Set<XPackFeatureSet> featureSets;

    @Inject
    public TransportXPackInfoAction(Settings settings, TransportService transportService,
                                    ActionFilters actionFilters, LicenseService licenseService, Set<XPackFeatureSet> featureSets) {
        super(settings, XPackInfoAction.NAME, transportService, actionFilters,
            XPackInfoRequest::new);
        this.licenseService = licenseService;
        this.featureSets = featureSets;
    }

    @Override
    protected void doExecute(XPackInfoRequest request, ActionListener<XPackInfoResponse> listener) {


        XPackInfoResponse.BuildInfo buildInfo = null;
        if (request.getCategories().contains(XPackInfoRequest.Category.BUILD)) {
            buildInfo = new XPackInfoResponse.BuildInfo(XPackBuild.CURRENT);
        }

        LicenseInfo licenseInfo = null;
        if (request.getCategories().contains(XPackInfoRequest.Category.LICENSE)) {
            License license = licenseService.getLicense();
            if (license != null) {
                licenseInfo = new LicenseInfo(license);
            }
        }

        XPackInfoResponse.FeatureSetsInfo featureSetsInfo = null;
        if (request.getCategories().contains(XPackInfoRequest.Category.FEATURES)) {
            Set<FeatureSet> featureSets = this.featureSets.stream().map(fs ->
                    new FeatureSet(fs.name(), request.isVerbose() ? fs.description() : null, fs.available(), fs.enabled(),
                            request.isVerbose() ? fs.nativeCodeInfo() : null))
                    .collect(Collectors.toSet());
            featureSetsInfo = new XPackInfoResponse.FeatureSetsInfo(featureSets);
        }

        listener.onResponse(new XPackInfoResponse(buildInfo, licenseInfo, featureSetsInfo));
    }
}
