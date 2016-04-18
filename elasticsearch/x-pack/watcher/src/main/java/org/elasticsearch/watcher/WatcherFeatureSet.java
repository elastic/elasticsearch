/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.XPackFeatureSet;

/**
 *
 */
public class WatcherFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final WatcherLicensee licensee;

    @Inject
    public WatcherFeatureSet(Settings settings, @Nullable WatcherLicensee licensee) {
        this.enabled = Watcher.enabled(settings);
        this.licensee = licensee;
    }

    @Override
    public String name() {
        return Watcher.NAME;
    }

    @Override
    public String description() {
        return "Alerting, Notification and Automation for the Elastic Stack";
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
