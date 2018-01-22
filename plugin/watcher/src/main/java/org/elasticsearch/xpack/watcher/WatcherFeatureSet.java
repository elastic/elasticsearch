/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.XPackField;

public class WatcherFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final WatcherService watcherService;

    @Inject
    public WatcherFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, @Nullable WatcherService watcherService) {
        this.watcherService = watcherService;
        this.enabled = XPackSettings.WATCHER_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.WATCHER;
    }

    @Override
    public String description() {
        return "Alerting, Notification and Automation for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isWatcherAllowed();
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
        listener.onResponse(
                new WatcherFeatureSetUsage(available(), enabled(), 
                watcherService != null ? watcherService.usageStats() : Collections.emptyMap()));
    }

}
