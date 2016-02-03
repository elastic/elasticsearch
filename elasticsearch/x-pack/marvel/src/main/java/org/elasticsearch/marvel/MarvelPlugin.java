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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.marvel.agent.AgentService;
import org.elasticsearch.marvel.agent.collector.CollectorModule;
import org.elasticsearch.marvel.agent.exporter.ExporterModule;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.renderer.RendererModule;
import org.elasticsearch.marvel.agent.settings.MarvelModule;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.cleaner.CleanerService;
import org.elasticsearch.marvel.license.LicenseModule;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.marvel.shield.MarvelShieldModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class MarvelPlugin extends Plugin {

    private static final ESLogger logger = Loggers.getLogger(XPackPlugin.class);

    public static final String NAME = "marvel";
    public static final String ENABLED = NAME + ".enabled";
    public static final Setting<String> INDEX_MARVEL_VERSION_SETTING = new Setting<>("index.marvel.plugin.version", "", Function.identity(), false, Setting.Scope.INDEX);
    public static final Setting<String> INDEX_MARVEL_TEMPLATE_VERSION_SETTING = new Setting<>("index.marvel.template.version", "", Function.identity(), false, Setting.Scope.INDEX);
    public static final String TRIBE_NAME_SETTING = "tribe.name";

    private final Settings settings;
    private final boolean enabled;

    public MarvelPlugin(Settings settings) {
        this.settings = settings;
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

    boolean isEnabled() {
        return enabled;
    }

    @Override
    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();

        // Always load the security integration for tribe nodes.
        // This is useful if the tribe node is connected to a
        // protected monitored cluster: __marvel_user operations must be allowed.
        if (enabled || isTribeNode(settings) || isTribeClientNode(settings)) {
            modules.add(new MarvelShieldModule(settings));
        }

        if (enabled) {
            modules.add(new MarvelModule());
            modules.add(new LicenseModule());
            modules.add(new CollectorModule());
            modules.add(new ExporterModule(settings));
            modules.add(new RendererModule());
        }
        return Collections.unmodifiableList(modules);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (!enabled) {
            return Collections.emptyList();
        }
        return Arrays.<Class<? extends LifecycleComponent>>asList(MarvelLicensee.class,
                AgentService.class,
                CleanerService.class);
    }

    public static boolean marvelEnabled(Settings settings) {
        if (!"node".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()))) {
            logger.trace("marvel cannot be started on a transport client");
            return false;
        }
        // By default, marvel is disabled on tribe nodes
        return settings.getAsBoolean(ENABLED, !isTribeNode(settings) && !isTribeClientNode(settings));
    }

    static boolean isTribeNode(Settings settings) {
        if (settings.getGroups("tribe", true).isEmpty() == false) {
            logger.trace("detecting tribe node");
            return true;
        }
        return false;
    }

    static boolean isTribeClientNode(Settings settings) {
        String tribeName = settings.get(TRIBE_NAME_SETTING);
        if (tribeName != null) {
            logger.trace("detecting tribe client node [{}]", tribeName);
            return true;
        }
        return false;
    }

    public void onModule(SettingsModule module) {
        module.registerSetting(Exporters.EXPORTERS_SETTING);
        module.registerSetting(MarvelSettings.INDICES_SETTING);
        module.registerSetting(MarvelSettings.INTERVAL_SETTING);
        module.registerSetting(MarvelSettings.INDEX_RECOVERY_TIMEOUT_SETTING);
        module.registerSetting(MarvelSettings.INDEX_STATS_TIMEOUT_SETTING);
        module.registerSetting(MarvelSettings.INDICES_STATS_TIMEOUT_SETTING);
        module.registerSetting(MarvelSettings.INDEX_RECOVERY_ACTIVE_ONLY_SETTING);
        module.registerSetting(MarvelSettings.COLLECTORS_SETTING);
        module.registerSetting(MarvelSettings.CLUSTER_STATE_TIMEOUT_SETTING);
        module.registerSetting(MarvelSettings.CLUSTER_STATS_TIMEOUT_SETTING);
        module.registerSetting(CleanerService.HISTORY_SETTING);
        module.registerSetting(INDEX_MARVEL_VERSION_SETTING);
        module.registerSetting(INDEX_MARVEL_TEMPLATE_VERSION_SETTING);
        // TODO convert these settings to where they belong
        module.registerSetting(Setting.simpleString("marvel.agent.exporter.es.ssl.truststore.password", false, Setting.Scope.CLUSTER));
        module.registerSetting(Setting.boolSetting("marvel.enabled", false, false, Setting.Scope.CLUSTER));
    }
}
