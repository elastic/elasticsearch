/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.collector.CollectorModule;
import org.elasticsearch.marvel.agent.exporter.ExporterModule;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.marvel.client.MonitoringClientModule;
import org.elasticsearch.marvel.license.LicenseModule;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.marvel.support.init.proxy.MonitoringClientProxy;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.common.init.LazyInitializationModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Marvel {

    private static final ESLogger logger = Loggers.getLogger(XPackPlugin.class);

    public static final String NAME = "monitoring";

    private final Settings settings;
    private final boolean enabled;

    public Marvel(Settings settings) {
        this.settings = settings;
        this.enabled = enabled(settings);
    }

    boolean isEnabled() {
        return enabled;
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();

        if (enabled) {
            modules.add(new MarvelModule());
            modules.add(new LicenseModule());
            modules.add(new CollectorModule());
            modules.add(new ExporterModule(settings));
            modules.add(new MonitoringClientModule());
        }
        return Collections.unmodifiableList(modules);
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (enabled == false) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(MarvelLicensee.class,
                AgentService.class,
                CleanerService.class);
    }

    public static boolean enabled(Settings settings) {
        if ("node".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey())) == false) {
            logger.trace("monitoring cannot be started on a transport client");
            return false;
        }
        return MarvelSettings.ENABLED.get(settings);
    }

    public void onModule(SettingsModule module) {
        MarvelSettings.register(module);
    }

    public void onModule(LazyInitializationModule module) {
        if (enabled) {
            module.registerLazyInitializable(MonitoringClientProxy.class);
        }
    }
}
