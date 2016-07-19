/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;


import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;


public class WatcherModule extends AbstractModule {

    private final boolean enabled;
    private final boolean transportClientMode;

    public WatcherModule(boolean enabled, boolean transportClientMode) {
        this.enabled = enabled;
        this.transportClientMode = transportClientMode;
    }

    @Override
    protected void configure() {
        if (transportClientMode) {
            return;
        }

        if (enabled == false) {
            // watcher service must be null, so that the watcher feature set can be instantiated even if watcher is not enabled
            bind(WatcherService.class).toProvider(Providers.of(null));
        } else {
            bind(WatcherLifeCycleService.class).asEagerSingleton();
            bind(WatcherIndexTemplateRegistry.class).asEagerSingleton();
        }

        XPackPlugin.bindFeatureSet(binder(), WatcherFeatureSet.class);
    }
}
