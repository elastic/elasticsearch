/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.common.inject.AbstractModule;

/**
 */
public class HistoryModule extends AbstractModule {

    private final Class<? extends AlertsExecutor> executorClass;

    public HistoryModule() {
        this(InternalAlertsExecutor.class);
    }

    protected HistoryModule(Class<? extends AlertsExecutor> executorClass) {
        this.executorClass = executorClass;
    }

    @Override
    protected void configure() {
        bind(FiredAlert.Parser.class).asEagerSingleton();
        bind(HistoryStore.class).asEagerSingleton();
        bind(HistoryService.class).asEagerSingleton();
        bind(executorClass).asEagerSingleton();
        bind(AlertsExecutor.class).to(executorClass);
    }
}
