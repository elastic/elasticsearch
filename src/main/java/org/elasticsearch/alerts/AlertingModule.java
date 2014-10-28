/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.rest.AlertRestHandler;
import org.elasticsearch.alerts.scheduler.AlertScheduler;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.common.inject.AbstractModule;

public class AlertingModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(AlertManager.class).asEagerSingleton();
        bind(TriggerManager.class).asEagerSingleton();
        bind(AlertScheduler.class).asEagerSingleton();
        bind(AlertActionRegistry.class).asEagerSingleton();
        bind(AlertRestHandler.class).asEagerSingleton();
    }

}
