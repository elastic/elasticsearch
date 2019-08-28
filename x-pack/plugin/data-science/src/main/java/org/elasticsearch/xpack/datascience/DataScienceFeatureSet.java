/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datascience;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.datascience.DataScienceFeatureSetUsage;
import org.elasticsearch.xpack.core.datascience.action.DataScienceStatsAction;

import java.util.Map;

public class DataScienceFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private Client client;

    @Inject
    public DataScienceFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, Client client) {
        this.enabled = XPackSettings.DATA_SCIENCE_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    public String name() {
        return XPackField.DATA_SCIENCE;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isDataScienceAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        if (enabled) {
           DataScienceStatsAction.Request request = new DataScienceStatsAction.Request();
           client.execute(DataScienceStatsAction.INSTANCE, request,
               ActionListener.wrap(r -> listener.onResponse(new DataScienceFeatureSetUsage(available(), enabled(), r)),
                   listener::onFailure));

        } else {
            listener.onResponse(new DataScienceFeatureSetUsage(available(), enabled(), null));
        }
    }
}
