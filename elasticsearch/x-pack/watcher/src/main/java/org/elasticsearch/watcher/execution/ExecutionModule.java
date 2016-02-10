/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.watcher.trigger.TriggerEngine;

/**
 */
public class ExecutionModule extends AbstractModule {

    private final Class<? extends WatchExecutor> executorClass;
    private final Class<? extends TriggerEngine.Listener> triggerEngineListenerClass;

    public ExecutionModule() {
        this(InternalWatchExecutor.class, AsyncTriggerListener.class);
    }

    protected ExecutionModule(Class<? extends WatchExecutor> executorClass,
                              Class<? extends TriggerEngine.Listener> triggerEngineListenerClass) {
        this.executorClass = executorClass;
        this.triggerEngineListenerClass = triggerEngineListenerClass;
    }

    @Override
    protected void configure() {
        bind(TriggeredWatch.Parser.class).asEagerSingleton();
        bind(TriggeredWatchStore.class).asEagerSingleton();
        bind(ExecutionService.class).asEagerSingleton();
        bind(executorClass).asEagerSingleton();
        bind(triggerEngineListenerClass).asEagerSingleton();
        bind(WatchExecutor.class).to(executorClass);
    }
}
