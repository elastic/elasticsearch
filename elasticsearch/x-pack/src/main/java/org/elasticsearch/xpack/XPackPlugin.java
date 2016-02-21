/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.license.plugin.Licensing;
import org.elasticsearch.marvel.Marvel;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.shield.Shield;
import org.elasticsearch.watcher.Watcher;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;

public class XPackPlugin extends Plugin {

    public static final String NAME = "xpack";

    // TODO: clean up this library to not ask for write access to all system properties!
    static {
        // invoke this clinit in unbound with permissions to access all system properties
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    try {
                        Class.forName("com.unboundid.util.Debug");
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
            });
            // TODO: fix gradle to add all shield resources (plugin metadata) to test classpath
            // of watcher plugin, which depends on it directly. This prevents these plugins
            // from being initialized correctly by the test framework, and means we have to
            // have this leniency.
        } catch (ExceptionInInitializerError bogus) {
            if (bogus.getCause() instanceof SecurityException == false) {
                throw bogus; // some other bug
            }
        }
    }

    protected final Settings settings;

    protected Licensing licensing;
    protected Shield shield;
    protected Marvel marvel;
    protected Watcher watcher;

    public XPackPlugin(Settings settings) {
        this.settings = settings;
        this.licensing = new Licensing(settings);
        this.shield = new Shield(settings);
        this.marvel = new Marvel(settings);
        this.watcher = new Watcher(settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public String description() {
        return "Elastic X-Pack";
    }

    @Override
    public Collection<Module> nodeModules() {
        ArrayList<Module> modules = new ArrayList<>();
        modules.addAll(licensing.nodeModules());
        modules.addAll(shield.nodeModules());
        modules.addAll(watcher.nodeModules());
        modules.addAll(marvel.nodeModules());
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        ArrayList<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.addAll(licensing.nodeServices());
        services.addAll(shield.nodeServices());
        services.addAll(watcher.nodeServices());
        services.addAll(marvel.nodeServices());
        return services;
    }

    @Override
    public Settings additionalSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(shield.additionalSettings());
        builder.put(watcher.additionalSettings());
        return builder.build();
    }

    public void onModule(ScriptModule module) {
        watcher.onModule(module);
    }

    public void onModule(SettingsModule module) {

        // we add the `xpack.version` setting to all internal indices
        module.registerSetting(Setting.simpleString("index.xpack.version", false, Setting.Scope.INDEX));

        shield.onModule(module);
        marvel.onModule(module);
        watcher.onModule(module);
        licensing.onModule(module);
    }

    public void onModule(NetworkModule module) {
        licensing.onModule(module);
        shield.onModule(module);
        watcher.onModule(module);
    }

    public void onModule(ActionModule module) {
        licensing.onModule(module);
        shield.onModule(module);
        watcher.onModule(module);
    }

    public void onIndexModule(IndexModule module) {
        shield.onIndexModule(module);
    }

    public static boolean transportClientMode(Settings settings) {
        return !"node".equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
    }

    public static boolean isTribeNode(Settings settings) {
        return settings.getGroups("tribe", true).isEmpty() == false;
    }
    public static boolean isTribeClientNode(Settings settings) {
        return settings.get("tribe.name") != null;
    }

    public static Path resolveConfigFile(Environment env, String name) {
        return env.configFile().resolve(NAME).resolve(name);
    }

    /**
     * A consistent way to enable disable features using the following setting:
     *
     *          {@code "xpack.<feature>.enabled": true | false}
     *
     *  Also supports the following setting as a fallback (for BWC with 1.x/2.x):
     *
     *          {@code "<feature>.enabled": true | false}
     */
    public static boolean featureEnabled(Settings settings, String featureName, boolean defaultValue) {
        return settings.getAsBoolean(featureEnabledSetting(featureName),
                settings.getAsBoolean(legacyFeatureEnabledSetting(featureName), defaultValue)); // for bwc
    }

    public static String featureEnabledSetting(String featureName) {
        return featureSettingPrefix(featureName) + ".enabled";
    }

    public static String featureSettingPrefix(String featureName) {
        return NAME + "." + featureName;
    }

    public static String legacyFeatureEnabledSetting(String featureName) {
        return featureName + ".enabled";
    }

    /**
     * A consistent way to register the settings used to enable disable features, supporting the following format:
     *
     *          {@code "xpack.<feature>.enabled": true | false}
     *
     *  Also supports the following setting as a fallback (for BWC with 1.x/2.x):
     *
     *          {@code "<feature>.enabled": true | false}
     */
    public static void registerFeatureEnabledSettings(SettingsModule settingsModule, String featureName, boolean defaultValue) {
        settingsModule.registerSetting(Setting.boolSetting(featureEnabledSetting(featureName), defaultValue, false, Setting.Scope.CLUSTER));
        settingsModule.registerSetting(Setting.boolSetting(legacyFeatureEnabledSetting(featureName),
                defaultValue, false, Setting.Scope.CLUSTER));
    }
}
