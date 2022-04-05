/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackBuild;
import org.elasticsearch.xpack.core.XPackFeatureSet;

import java.util.Set;
import java.util.stream.Collectors;

public class TransportXPackInfoAction extends HandledTransportAction<XPackInfoRequest, XPackInfoResponse> {

    private final LicenseService licenseService;
    private final Set<XPackFeatureSet> featureSets;

    @Inject
    public TransportXPackInfoAction(
        TransportService transportService,
        ActionFilters actionFilters,
        LicenseService licenseService,
        Set<XPackFeatureSet> featureSets
    ) {
        super(XPackInfoAction.NAME, transportService, actionFilters, XPackInfoRequest::new);
        this.licenseService = licenseService;
        this.featureSets = featureSets;
    }

    @Override
    protected void doExecute(Task task, XPackInfoRequest request, ActionListener<XPackInfoResponse> listener) {

        XPackInfoResponse.BuildInfo buildInfo = null;
        if (request.getCategories().contains(XPackInfoRequest.Category.BUILD)) {
            buildInfo = new XPackInfoResponse.BuildInfo(XPackBuild.CURRENT.shortHash(), XPackBuild.CURRENT.date());
        }

        LicenseInfo licenseInfo = null;
        if (request.getCategories().contains(XPackInfoRequest.Category.LICENSE)) {
            License license = licenseService.getLicense();
            if (license != null) {
                String type = license.type();
                License.OperationMode mode = license.operationMode();
                if (request.getLicenseVersion() < License.VERSION_ENTERPRISE) {
                    if (License.LicenseType.ENTERPRISE.getTypeName().equals(type)) {
                        type = License.LicenseType.PLATINUM.getTypeName();
                    }
                    if (mode == License.OperationMode.ENTERPRISE) {
                        mode = License.OperationMode.PLATINUM;
                    }
                }
                licenseInfo = new LicenseInfo(
                    license.uid(),
                    type,
                    mode.description(),
                    LicenseService.status(license),
                    LicenseService.getExpiryDate(license)
                );
            }
        }

        XPackInfoResponse.FeatureSetsInfo featureSetsInfo = null;
        if (request.getCategories().contains(XPackInfoRequest.Category.FEATURES)) {
            Set<FeatureSet> featureSets = this.featureSets.stream()
                .map(fs -> new FeatureSet(fs.name(), fs.available(), fs.enabled(), request.isVerbose() ? fs.nativeCodeInfo() : null))
                .collect(Collectors.toSet());
            featureSetsInfo = new XPackInfoResponse.FeatureSetsInfo(featureSets);
        }

        listener.onResponse(new XPackInfoResponse(buildInfo, licenseInfo, featureSetsInfo));
    }
}
