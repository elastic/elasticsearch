/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;


import org.elasticsearch.watcher.actions.ActionModule;
import org.elasticsearch.watcher.client.WatcherClientModule;
import org.elasticsearch.watcher.condition.ConditionModule;
import org.elasticsearch.watcher.history.HistoryModule;
import org.elasticsearch.watcher.input.InputModule;
import org.elasticsearch.watcher.rest.WatcherRestModule;
import org.elasticsearch.watcher.scheduler.SchedulerModule;
import org.elasticsearch.watcher.support.TemplateUtils;
import org.elasticsearch.watcher.support.clock.ClockModule;
import org.elasticsearch.watcher.support.init.InitializingModule;
import org.elasticsearch.watcher.support.template.TemplateModule;
import org.elasticsearch.watcher.transform.TransformModule;
import org.elasticsearch.watcher.transport.WatcherTransportModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.watcher.watch.WatchModule;


public class WatcherModule extends AbstractModule implements SpawnModules {

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(
                new InitializingModule(),
                new WatchModule(),
                new TemplateModule(),
                new ClockModule(),
                new WatcherClientModule(),
                new TransformModule(),
                new WatcherRestModule(),
                new SchedulerModule(),
                new WatcherTransportModule(),
                new ConditionModule(),
                new InputModule(),
                new ActionModule(),
                new HistoryModule());
    }

    @Override
    protected void configure() {
        bind(WatcherLifeCycleService.class).asEagerSingleton();
        bind(TemplateUtils.class).asEagerSingleton();
    }

}
