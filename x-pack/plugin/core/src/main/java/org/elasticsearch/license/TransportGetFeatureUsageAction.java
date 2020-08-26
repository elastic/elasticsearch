/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import java.util.Locale;
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
        Map<XPackLicenseState.Feature, Long> featureUsage = licenseState.getLastUsed();
        List<GetFeatureUsageResponse.FeatureUsageInfo> usageInfos = new ArrayList<>();
        for (var entry : featureUsage.entrySet()) {
            XPackLicenseState.Feature feature = entry.getKey();
            String name = feature.name().toLowerCase(Locale.ROOT);
            ZonedDateTime lastUsedTime = Instant.ofEpochMilli(entry.getValue()).atZone(ZoneOffset.UTC);
            String licenseLevel = feature.minimumOperationMode.name().toLowerCase(Locale.ROOT);
            usageInfos.add(new GetFeatureUsageResponse.FeatureUsageInfo(name, lastUsedTime, licenseLevel));
        }
        listener.onResponse(new GetFeatureUsageResponse(usageInfos));
    }
}
