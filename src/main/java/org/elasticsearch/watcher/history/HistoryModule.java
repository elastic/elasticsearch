/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.common.inject.AbstractModule;

/**
 */
public class HistoryModule extends AbstractModule {

    private final Class<? extends WatchExecutor> executorClass;

    public HistoryModule() {
        this(InternalWatchExecutor.class);
    }

    protected HistoryModule(Class<? extends WatchExecutor> executorClass) {
        this.executorClass = executorClass;
    }

    @Override
    protected void configure() {
        bind(WatchRecord.Parser.class).asEagerSingleton();
        bind(HistoryStore.class).asEagerSingleton();
        bind(HistoryService.class).asEagerSingleton();
        bind(executorClass).asEagerSingleton();
        bind(WatchExecutor.class).to(executorClass);
    }
}
