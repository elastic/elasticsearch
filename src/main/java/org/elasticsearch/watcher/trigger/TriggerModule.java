/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger;

import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.watcher.trigger.schedule.ScheduleModule;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class TriggerModule extends AbstractModule implements SpawnModules {

    private final Set<Class<? extends TriggerEngine>> engines = new HashSet<>();

    public TriggerModule() {
        registerStandardEngines();
    }

    public void registerEngine(Class<? extends TriggerEngine> engineType) {
        engines.add(engineType);
    }

    protected void registerStandardEngines() {
        registerEngine(ScheduleModule.triggerEngineType());
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableSet.<Module>of(new ScheduleModule());
    }

    @Override
    protected void configure() {

        Multibinder<TriggerEngine> mbinder = Multibinder.newSetBinder(binder(), TriggerEngine.class);
        for (Class<? extends TriggerEngine> engine : engines) {
            bind(engine).asEagerSingleton();
            mbinder.addBinding().to(engine);
        }

        bind(TriggerService.class).asEagerSingleton();
    }
}
