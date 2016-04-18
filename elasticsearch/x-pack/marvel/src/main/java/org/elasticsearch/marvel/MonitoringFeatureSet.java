/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackFeatureSet;

/**
 *
 */
public class MonitoringFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final MonitoringLicensee licensee;

    @Inject
    public MonitoringFeatureSet(Settings settings, @Nullable MonitoringLicensee licensee) {
        this.enabled = MonitoringSettings.ENABLED.get(settings);
        this.licensee = licensee;
    }

    @Override
    public String name() {
        return Monitoring.NAME;
    }

    @Override
    public String description() {
        return "Monitoring for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licensee != null && licensee.available();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }
}
