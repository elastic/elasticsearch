/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackBuild;
import org.elasticsearch.xpack.core.common.IteratingActionListener;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportXPackInfoAction extends HandledTransportAction<XPackInfoRequest, XPackInfoResponse> {

    private final LicenseService licenseService;
    private final NodeClient client;
    private final ThreadPool threadPool;

    @Inject
    public TransportXPackInfoAction(TransportService transportService, ActionFilters actionFilters, LicenseService licenseService,
                                    NodeClient client, ThreadPool threadPool) {
        super(XPackInfoAction.NAME, transportService, actionFilters,
            XPackInfoRequest::new);
        this.licenseService = licenseService;
        this.client = client;
        this.threadPool = threadPool;
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
                licenseInfo = new LicenseInfo(license.uid(), license.type(), license.operationMode().description(),
                        license.status(), license.expiryDate());
            }
        }

        if (request.getCategories().contains(XPackInfoRequest.Category.FEATURES)) {
            final var position = new AtomicInteger(0);
            var featureSets = new AtomicReferenceArray<FeatureSet>(XPackInfoFeatureAction.ALL.size());
            final XPackInfoResponse.BuildInfo finalBuildInfo = buildInfo;
            final LicenseInfo finalLicenseInfo = licenseInfo;

            ActionListener<Void> finalListener = ActionListener.wrap(ignore -> {
                var featureSetsSet = new HashSet<FeatureSet>(featureSets.length());
                for (int i = 0; i < featureSets.length(); i++) {
                    featureSetsSet.add(featureSets.get(i));
                }
                var featureSetsInfo = new XPackInfoResponse.FeatureSetsInfo(featureSetsSet);
                listener.onResponse(new XPackInfoResponse(finalBuildInfo, finalLicenseInfo, featureSetsInfo));
            }, listener::onFailure);

            new IteratingActionListener<>(finalListener, (infoAction, iteratingListener) ->
                client.executeLocally(infoAction, request, ActionListener.wrap(response ->
                    featureSets.set(position.getAndIncrement(), response.getInfo()), iteratingListener::onFailure)),
                XPackInfoFeatureAction.ALL,
                threadPool.getThreadContext()).run();
        } else {
            listener.onResponse(new XPackInfoResponse(buildInfo, licenseInfo, null));
        }
    }
}
