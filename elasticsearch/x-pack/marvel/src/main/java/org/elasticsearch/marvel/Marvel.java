/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.marvel.action.MonitoringBulkAction;
import org.elasticsearch.marvel.action.TransportMonitoringBulkAction;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.collector.CollectorModule;
import org.elasticsearch.marvel.agent.exporter.ExporterModule;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.marvel.client.MonitoringClientModule;
import org.elasticsearch.marvel.license.LicenseModule;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.marvel.rest.action.RestMonitoringBulkAction;
import org.elasticsearch.marvel.support.init.proxy.MonitoringClientProxy;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.common.init.LazyInitializationModule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This class activates/deactivates the monitoring modules depending if we're running a node client, transport client or tribe client:
 * - node clients: all modules are binded
 * - transport clients: only action/transport actions are binded
 * - tribe clients: everything is disables by default but can be enabled per tribe cluster
 */
public class Marvel {

    public static final String NAME = "monitoring";

    private final Settings settings;
    private final boolean enabled;
    private final boolean transportClientMode;

    public Marvel(Settings settings) {
        this.settings = settings;
        this.enabled = MarvelSettings.ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    boolean isEnabled() {
        return enabled;
    }

    boolean isTransportClient() {
        return transportClientMode;
    }

    public Collection<Module> nodeModules() {
        if (enabled == false || transportClientMode) {
            return Collections.emptyList();
        }

        return Arrays.<Module>asList(
                new MarvelModule(),
                new LicenseModule(),
                new CollectorModule(),
                new ExporterModule(settings),
                new MonitoringClientModule());
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled == false || transportClientMode) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(MarvelLicensee.class,
                AgentService.class,
                CleanerService.class);
    }

    public void onModule(SettingsModule module) {
        MarvelSettings.register(module);
    }

    public void onModule(ActionModule module) {
        if (enabled) {
            module.registerAction(MonitoringBulkAction.INSTANCE, TransportMonitoringBulkAction.class);
        }
    }

    public void onModule(NetworkModule module) {
        if (enabled && transportClientMode == false) {
            module.registerRestHandler(RestMonitoringBulkAction.class);
        }
    }

    public void onModule(LazyInitializationModule module) {
        if (enabled) {
            module.registerLazyInitializable(MonitoringClientProxy.class);
        }
    }
}
