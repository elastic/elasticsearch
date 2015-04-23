/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.watcher.WatcherService;

/**
 *
 */
public class WatchModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Watch.Parser.class).asEagerSingleton();
        bind(WatchLockService.class).asEagerSingleton();
        bind(WatcherService.class).asEagerSingleton();
        bind(WatchStore.class).asEagerSingleton();
    }
}
