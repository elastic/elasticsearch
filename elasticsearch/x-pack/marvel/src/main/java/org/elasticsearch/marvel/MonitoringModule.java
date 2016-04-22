/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.xpack.XPackPlugin;

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
            bind(MonitoringLicensee.class).asEagerSingleton();
            bind(MonitoringSettings.class).asEagerSingleton();
            bind(AgentService.class).asEagerSingleton();
            bind(CleanerService.class).asEagerSingleton();
        } else {
            bind(MonitoringLicensee.class).toProvider(Providers.of(null));
        }
    }
}
