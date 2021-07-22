/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransportGetFeatureUsageAction extends HandledTransportAction<GetFeatureUsageRequest, GetFeatureUsageResponse> {

    public static final ActionType<GetFeatureUsageResponse> TYPE =
        new ActionType<>("cluster:admin/xpack/license/feature_usage", GetFeatureUsageResponse::new);

    private final XPackLicenseState licenseState;

    @Inject
    public TransportGetFeatureUsageAction(TransportService transportService, ActionFilters actionFilters,
                                          XPackLicenseState licenseState) {
        super(TYPE.name(), transportService, actionFilters, GetFeatureUsageRequest::new);
        this.licenseState = licenseState;
    }


    @Override
    protected void doExecute(Task task, GetFeatureUsageRequest request, ActionListener<GetFeatureUsageResponse> listener) {
        Map<XPackLicenseState.FeatureUsage, Long> featureUsage = licenseState.getLastUsed();
        List<GetFeatureUsageResponse.FeatureUsageInfo> usageInfos = new ArrayList<>(featureUsage.size());
        featureUsage.forEach((usage, lastUsed) -> {
            ZonedDateTime lastUsedTime = Instant.ofEpochMilli(lastUsed).atZone(ZoneOffset.UTC);
            usageInfos.add(
                new GetFeatureUsageResponse.FeatureUsageInfo(
                    usage.featureName(),
                    lastUsedTime,
                    usage.contextName(),
                    usage.minimumOperationMode().description()
                )
            );
        });
        listener.onResponse(new GetFeatureUsageResponse(usageInfos));
    }
}
