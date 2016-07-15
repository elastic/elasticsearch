/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.monitoring.agent.AgentService;
import org.elasticsearch.xpack.monitoring.cleaner.CleanerService;

public class MonitoringModule extends AbstractModule {

    private final boolean enabled;
    private final boolean transportClientMode;

    public MonitoringModule(boolean enabled, boolean transportClientMode) {
        this.enabled = enabled;
        this.transportClientMode = transportClientMode;
    }

    @Override
    protected void configure() {
        XPackPlugin.bindFeatureSet(binder(), MonitoringFeatureSet.class);

        if (enabled && transportClientMode == false) {
            bind(MonitoringSettings.class).asEagerSingleton();
            bind(AgentService.class).asEagerSingleton();
            bind(CleanerService.class).asEagerSingleton();
        } else if (transportClientMode) {
            bind(MonitoringLicensee.class).toProvider(Providers.of(null));
        }
    }
}
