/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;


import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.actions.ActionModule;
import org.elasticsearch.watcher.client.WatcherClientModule;
import org.elasticsearch.watcher.condition.ConditionModule;
import org.elasticsearch.watcher.execution.ExecutionModule;
import org.elasticsearch.watcher.history.HistoryModule;
import org.elasticsearch.watcher.input.InputModule;
import org.elasticsearch.watcher.license.LicenseModule;
import org.elasticsearch.watcher.rest.WatcherRestModule;
import org.elasticsearch.watcher.shield.WatcherShieldModule;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry.TemplateConfig;
import org.elasticsearch.watcher.support.clock.ClockModule;
import org.elasticsearch.watcher.support.http.HttpClientModule;
import org.elasticsearch.watcher.support.init.InitializingModule;
import org.elasticsearch.watcher.support.secret.SecretModule;
import org.elasticsearch.watcher.support.template.TemplateModule;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;
import org.elasticsearch.watcher.transform.TransformModule;
import org.elasticsearch.watcher.transport.WatcherTransportModule;
import org.elasticsearch.watcher.trigger.TriggerModule;
import org.elasticsearch.watcher.watch.WatchModule;

import java.util.Arrays;


public class WatcherModule extends AbstractModule implements SpawnModules {

    public static final String HISTORY_TEMPLATE_NAME = "watch_history";
    public static final String TRIGGERED_TEMPLATE_NAME = "triggered_watches";
    public static final String WATCHES_TEMPLATE_NAME = "watches";

    protected final Settings settings;

    public WatcherModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return Arrays.asList(
                new InitializingModule(),
                new LicenseModule(),
                new WatchModule(),
                new TemplateModule(),
                new HttpClientModule(),
                new ClockModule(),
                new WatcherClientModule(),
                new TransformModule(),
                new WatcherRestModule(),
                new TriggerModule(settings),
                new WatcherTransportModule(),
                new ConditionModule(),
                new InputModule(),
                new ActionModule(),
                new HistoryModule(),
                new ExecutionModule(),
                new WatcherShieldModule(settings),
                new SecretModule(settings));
    }

    @Override
    protected void configure() {
        bind(WatcherLifeCycleService.class).asEagerSingleton();
        bind(WatcherSettingsValidation.class).asEagerSingleton();

        bind(WatcherIndexTemplateRegistry.class).asEagerSingleton();
        Multibinder<TemplateConfig> multibinder
                = Multibinder.newSetBinder(binder(), TemplateConfig.class);
        multibinder.addBinding().toInstance(new TemplateConfig(TRIGGERED_TEMPLATE_NAME, "watcher.triggered_watches.index"));
        multibinder.addBinding().toInstance(new TemplateConfig(HISTORY_TEMPLATE_NAME, "watcher.history.index"));
        multibinder.addBinding().toInstance(new TemplateConfig(WATCHES_TEMPLATE_NAME, "watcher.watches.index"));
    }

}
