/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.util.Map;

public class AnalyticsFeatureSet implements XPackFeatureSet {

    private final XPackLicenseState licenseState;
    private Client client;

    @Inject
    public AnalyticsFeatureSet(@Nullable XPackLicenseState licenseState, Client client) {
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    public String name() {
        return XPackField.ANALYTICS;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isAnalyticsAllowed();
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
       AnalyticsStatsAction.Request request = new AnalyticsStatsAction.Request();
       client.execute(AnalyticsStatsAction.INSTANCE, request,
           ActionListener.wrap(r -> listener.onResponse(new AnalyticsFeatureSetUsage(available(), enabled(), r)),
               listener::onFailure));
    }
}
