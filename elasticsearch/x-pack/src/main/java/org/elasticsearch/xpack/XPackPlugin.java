/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.watcher.WatcherPlugin;

import java.util.ArrayList;
import java.util.Collection;

public class XPackPlugin extends Plugin {

    public static final String NAME = "xpack";

    private final static ESLogger logger = Loggers.getLogger(XPackPlugin.class);

    protected final Settings settings;
    protected final ShieldPlugin shieldPlugin;
    protected final MarvelPlugin marvelPlugin;
    protected WatcherPlugin watcherPlugin;

    public XPackPlugin(Settings settings) {
        this.settings = settings;
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
        modules.addAll(shieldPlugin.nodeModules());
        modules.addAll(watcherPlugin.nodeModules());
        modules.addAll(marvelPlugin.nodeModules());
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        ArrayList<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.addAll(shieldPlugin.nodeServices());
        services.addAll(watcherPlugin.nodeServices());
        services.addAll(marvelPlugin.nodeServices());
        return services;
    }

    @Override
    public Settings additionalSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(shieldPlugin.additionalSettings());
        builder.put(watcherPlugin.additionalSettings());
        builder.put(marvelPlugin.additionalSettings());
        return builder.build();
    }

    public void onModule(ScriptModule module) {
        watcherPlugin.onModule(module);
    }

    public void onModule(ClusterModule module) {
        shieldPlugin.onModule(module);
        watcherPlugin.onModule(module);
        marvelPlugin.onModule(module);
    }

    public void onModule(RestModule module) {
        shieldPlugin.onModule(module);
        watcherPlugin.onModule(module);
    }

    public void onModule(ActionModule module) {
        shieldPlugin.onModule(module);
        watcherPlugin.onModule(module);
    }

    public void onModule(TransportModule module) {
        shieldPlugin.onModule(module);
    }

    // NOTE: The fact this signature takes a module is a hack, and effectively like the previous
    // processModule in the plugin api. The problem is tight coupling between watcher and shield.
    // We need to avoid trying to load the AuthorizationModule class unless we know shield integration
    // is enabled. This is a temporary solution until inter-plugin-communication can be worked out.
    public void onModule(Module module) {
        watcherPlugin.onModule(module);
        marvelPlugin.onModule(module);
    }

    public void onIndexService(IndexService indexService) {
        shieldPlugin.onIndexService(indexService);
        watcherPlugin.onIndexService(indexService);
        marvelPlugin.onIndexService(indexService);
    }

    public void onIndexModule(IndexModule module) {
        shieldPlugin.onIndexModule(module);
        watcherPlugin.onIndexModule(module);
        marvelPlugin.onIndexModule(module);
    }
}
