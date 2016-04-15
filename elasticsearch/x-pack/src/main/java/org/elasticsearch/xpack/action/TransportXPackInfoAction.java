/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackBuild;
import org.elasticsearch.xpack.action.XPackInfoResponse.LicenseInfo;

/**
 */
public class TransportXPackInfoAction extends HandledTransportAction<XPackInfoRequest, XPackInfoResponse> {

    private final LicensesService licensesService;

    @Inject
    public TransportXPackInfoAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                    LicensesService licensesService) {
        super(settings, XPackInfoAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                XPackInfoRequest::new);
        this.licensesService = licensesService;
    }

    @Override
    protected void doExecute(XPackInfoRequest request, ActionListener<XPackInfoResponse> listener) {
        XPackInfoResponse.BuildInfo buildInfo = new XPackInfoResponse.BuildInfo(XPackBuild.CURRENT);
        License license = licensesService.getLicense();
        LicenseInfo licenseInfo = license != null ? new LicenseInfo(license) : null;
        XPackInfoResponse response = new XPackInfoResponse(buildInfo, licenseInfo);
        listener.onResponse(response);
    }
}
