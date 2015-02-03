/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.actions.AlertActionService;
import org.elasticsearch.alerts.client.AlertsClientModule;
import org.elasticsearch.alerts.rest.AlertsRestModule;
import org.elasticsearch.alerts.scheduler.AlertsSchedulerModule;
import org.elasticsearch.alerts.support.TemplateUtils;
import org.elasticsearch.alerts.support.init.InitializingModule;
import org.elasticsearch.alerts.transport.AlertsTransportModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;


public class AlertsModule extends AbstractModule implements SpawnModules {

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(
                new InitializingModule(),
                new AlertsSchedulerModule(),
                new AlertsTransportModule(),
                new AlertsClientModule(),
                new AlertsRestModule());
    }

    @Override
    protected void configure() {

        // Core components
        bind(TemplateUtils.class).asEagerSingleton();
        bind(AlertsStore.class).asEagerSingleton();
        bind(AlertsService.class).asEagerSingleton();
        bind(AlertActionService.class).asEagerSingleton();
        bind(AlertActionRegistry.class).asEagerSingleton();
        bind(ConfigurationService.class).asEagerSingleton();

    }

}
