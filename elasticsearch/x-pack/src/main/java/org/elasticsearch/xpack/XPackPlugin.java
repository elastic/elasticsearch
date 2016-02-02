/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.watcher.WatcherPlugin;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;

public class XPackPlugin extends Plugin {

    public static final String NAME = "x-pack";

    private final static ESLogger logger = Loggers.getLogger(XPackPlugin.class);

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
    protected LicensePlugin licensePlugin;
    protected ShieldPlugin shieldPlugin;
    protected MarvelPlugin marvelPlugin;
    protected WatcherPlugin watcherPlugin;

    public XPackPlugin(Settings settings) {
        this.settings = settings;
        this.licensePlugin = new LicensePlugin(settings);
        this.shieldPlugin = new ShieldPlugin(settings);
        this.marvelPlugin = new MarvelPlugin(settings);
        this.watcherPlugin = new WatcherPlugin(settings);
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
        modules.addAll(licensePlugin.nodeModules());
        modules.addAll(shieldPlugin.nodeModules());
        modules.addAll(watcherPlugin.nodeModules());
        modules.addAll(marvelPlugin.nodeModules());
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        ArrayList<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.addAll(licensePlugin.nodeServices());
        services.addAll(shieldPlugin.nodeServices());
        services.addAll(watcherPlugin.nodeServices());
        services.addAll(marvelPlugin.nodeServices());
        return services;
    }

    @Override
    public Settings additionalSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(licensePlugin.additionalSettings());
        builder.put(shieldPlugin.additionalSettings());
        builder.put(watcherPlugin.additionalSettings());
        builder.put(marvelPlugin.additionalSettings());
        return builder.build();
    }

    public void onModule(ScriptModule module) {
        watcherPlugin.onModule(module);
    }

    public void onModule(SettingsModule module) {
        shieldPlugin.onModule(module);
        marvelPlugin.onModule(module);
        watcherPlugin.onModule(module);
    }

    public void onModule(NetworkModule module) {
        licensePlugin.onModule(module);
        shieldPlugin.onModule(module);
        watcherPlugin.onModule(module);
    }

    public void onModule(ActionModule module) {
        licensePlugin.onModule(module);
        shieldPlugin.onModule(module);
        watcherPlugin.onModule(module);
    }

    public void onIndexModule(IndexModule module) {
        shieldPlugin.onIndexModule(module);
        watcherPlugin.onIndexModule(module);
        marvelPlugin.onIndexModule(module);
    }
}
