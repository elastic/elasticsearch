/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackBuild;

import java.util.HashSet;
import java.util.List;

public class TransportXPackInfoAction extends HandledTransportAction<XPackInfoRequest, XPackInfoResponse> {

    private final LicenseService licenseService;
    private final NodeClient client;
    private final List<ActionType<XPackInfoFeatureResponse>> infoActions;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportXPackInfoAction(
        TransportService transportService,
        ActionFilters actionFilters,
        LicenseService licenseService,
        NodeClient client
    ) {
        super(XPackInfoAction.NAME, transportService, actionFilters, XPackInfoRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.licenseService = licenseService;
        this.client = client;
        this.infoActions = infoActions();
    }

    // overrideable for tests
    protected List<ActionType<XPackInfoFeatureResponse>> infoActions() {
        return XPackInfoFeatureAction.ALL;
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
                licenseInfo = new LicenseInfo(
                    license.uid(),
                    license.type(),
                    license.operationMode().description(),
                    LicenseUtils.status(license),
                    LicenseUtils.getExpiryDate(license)
                );
            }
        }

        FeatureSetsInfo featureSetsInfo = null;
        if (request.getCategories().contains(XPackInfoRequest.Category.FEATURES)) {
            var featureSets = new HashSet<FeatureSet>();
            for (var infoAction : infoActions) {
                // local actions are executed directly, not on a separate thread, so no thread safe collection is necessary
                client.executeLocally(
                    infoAction,
                    request,
                    listener.delegateFailureAndWrap((l, response) -> featureSets.add(response.getInfo()))
                );
            }
            featureSetsInfo = new FeatureSetsInfo(featureSets);
        }

        listener.onResponse(new XPackInfoResponse(buildInfo, licenseInfo, featureSetsInfo));
    }
}
