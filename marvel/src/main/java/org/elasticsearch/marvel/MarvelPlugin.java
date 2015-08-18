/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.exporter.HttpESExporter;
import org.elasticsearch.marvel.agent.settings.MarvelSetting;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.tribe.TribeService;

import java.util.Collection;

public class MarvelPlugin extends AbstractPlugin {

    private static final ESLogger logger = Loggers.getLogger(MarvelPlugin.class);

    public static final String NAME = "marvel";
    public static final String ENABLED = NAME + ".enabled";

    private final boolean enabled;

    public MarvelPlugin(Settings settings) {
        this.enabled = marvelEnabled(settings);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Marvel";
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        if (!enabled) {
            return ImmutableList.of();
        }
        return ImmutableList.<Class<? extends Module>>of(MarvelModule.class);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        if (!enabled) {
            return ImmutableList.of();
        }
        return ImmutableList.<Class<? extends LifecycleComponent>>of(LicenseService.class, AgentService.class);
    }

    public static boolean marvelEnabled(Settings settings) {
        String tribe = settings.get(TribeService.TRIBE_NAME);
        if (tribe != null) {
            logger.trace("marvel cannot be started on tribe node [{}]", tribe);
            return false;
        }

        if (!"node".equals(settings.get(Client.CLIENT_TYPE_SETTING))) {
            logger.trace("marvel cannot be started on a transport client");
            return false;
        }
        return settings.getAsBoolean(ENABLED, true);
    }

    public void onModule(ClusterModule module) {
        // HttpESExporter
        module.registerClusterDynamicSetting(HttpESExporter.SETTINGS_HOSTS, Validator.EMPTY);
        module.registerClusterDynamicSetting(HttpESExporter.SETTINGS_HOSTS + ".*", Validator.EMPTY);
        module.registerClusterDynamicSetting(HttpESExporter.SETTINGS_TIMEOUT, Validator.EMPTY);
        module.registerClusterDynamicSetting(HttpESExporter.SETTINGS_READ_TIMEOUT, Validator.EMPTY);
        module.registerClusterDynamicSetting(HttpESExporter.SETTINGS_SSL_HOSTNAME_VERIFICATION, Validator.EMPTY);

        // MarvelSettingsService
        for (MarvelSetting setting : MarvelSettings.dynamicSettings()) {
            module.registerClusterDynamicSetting(setting.dynamicSettingName(), setting.dynamicValidator());
        }
    }
}
