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


    public HistoryModule() {
    }


    @Override
    protected void configure() {
        bind(WatchRecord.Parser.class).asEagerSingleton();
        bind(HistoryStore.class).asEagerSingleton();
    }
}
