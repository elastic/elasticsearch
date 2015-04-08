/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.inject.AbstractModule;

/**
 */
public class ExecutionModule extends AbstractModule {

    private final Class<? extends WatchExecutor> executorClass;

    public ExecutionModule() {
        this(InternalWatchExecutor.class);
    }

    protected ExecutionModule(Class<? extends WatchExecutor> executorClass) {
        this.executorClass = executorClass;
    }

    @Override
    protected void configure() {
        bind(ExecutionService.class).asEagerSingleton();
        bind(executorClass).asEagerSingleton();
        bind(WatchExecutor.class).to(executorClass);
    }
}
