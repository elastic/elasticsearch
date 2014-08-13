/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.common.inject.AbstractModule;

public class AlertingModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(AlertManager.class).asEagerSingleton();
        bind(TriggerManager.class).asEagerSingleton();
        bind(AlertScheduler.class).asEagerSingleton();
        bind(AlertActionManager.class).asEagerSingleton();
        bind(AlertRestHandler.class).asEagerSingleton();
    }

}
