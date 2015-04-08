/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.watcher.transport.actions.ack.TransportAckWatchAction;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.watcher.transport.actions.delete.TransportDeleteWatchAction;
import org.elasticsearch.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.watcher.transport.actions.get.TransportGetWatchAction;
import org.elasticsearch.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.watcher.transport.actions.put.TransportPutWatchAction;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.watcher.transport.actions.execute.TransportExecuteWatchAction;
import org.elasticsearch.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.watcher.transport.actions.service.TransportWatcherServiceAction;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.watcher.transport.actions.stats.TransportWatcherStatsAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;

/**
 *
 */
public class WatcherTransportModule extends AbstractModule implements PreProcessModule {

    @Override
    public void processModule(Module module) {
        if (module instanceof ActionModule) {
            ActionModule actionModule = (ActionModule) module;
            actionModule.registerAction(PutWatchAction.INSTANCE, TransportPutWatchAction.class);
            actionModule.registerAction(DeleteWatchAction.INSTANCE, TransportDeleteWatchAction.class);
            actionModule.registerAction(GetWatchAction.INSTANCE, TransportGetWatchAction.class);
            actionModule.registerAction(WatcherStatsAction.INSTANCE, TransportWatcherStatsAction.class);
            actionModule.registerAction(AckWatchAction.INSTANCE, TransportAckWatchAction.class);
            actionModule.registerAction(WatcherServiceAction.INSTANCE, TransportWatcherServiceAction.class);
            actionModule.registerAction(ExecuteWatchAction.INSTANCE, TransportExecuteWatchAction.class);
        }
    }

    @Override
    protected void configure() {
    }

}
