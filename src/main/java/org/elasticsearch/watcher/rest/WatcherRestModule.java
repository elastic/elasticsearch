/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest;

import org.elasticsearch.watcher.rest.action.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.rest.RestModule;

/**
 *
 */
public class WatcherRestModule extends AbstractModule implements PreProcessModule {

    @Override
    public void processModule(Module module) {
        if (module instanceof RestModule) {
            RestModule restModule = (RestModule) module;
            restModule.addRestAction(RestPutWatchAction.class);
            restModule.addRestAction(RestDeleteWatchAction.class);
            restModule.addRestAction(RestWatcherStatsAction.class);
            restModule.addRestAction(RestWatcherInfoAction.class);
            restModule.addRestAction(RestGetWatchAction.class);
            restModule.addRestAction(RestWatchServiceAction.class);
            restModule.addRestAction(RestAckWatchAction.class);
            restModule.addRestAction(RestExecuteWatchAction.class);
        }
    }

    @Override
    protected void configure() {
    }
}
