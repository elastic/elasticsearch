/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;


import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry.TemplateConfig;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;


public class WatcherModule extends AbstractModule {

    public static final String HISTORY_TEMPLATE_NAME = "watch_history";
    public static final String TRIGGERED_TEMPLATE_NAME = "triggered_watches";
    public static final String WATCHES_TEMPLATE_NAME = "watches";

    public final static TemplateConfig[] TEMPLATE_CONFIGS = new TemplateConfig[]{
            new TemplateConfig(TRIGGERED_TEMPLATE_NAME, "watcher.triggered_watches.index"),
            new TemplateConfig(HISTORY_TEMPLATE_NAME, "watcher.history.index"),
            new TemplateConfig(WATCHES_TEMPLATE_NAME, "watcher.watches.index")
    };

    protected final Settings settings;

    public WatcherModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(WatcherLifeCycleService.class).asEagerSingleton();
        bind(WatcherSettingsValidation.class).asEagerSingleton();

        bind(WatcherIndexTemplateRegistry.class).asEagerSingleton();
        Multibinder<TemplateConfig> multibinder
                = Multibinder.newSetBinder(binder(), TemplateConfig.class);
        for (TemplateConfig templateConfig : TEMPLATE_CONFIGS) {
            multibinder.addBinding().toInstance(templateConfig);
        }
    }

}
