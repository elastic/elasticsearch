/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.execution.InternalWatchExecutor;

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

    public static Settings additionalSettings(Settings nodeSettings) {
        return InternalWatchExecutor.additionalSettings(nodeSettings);
    }
}
