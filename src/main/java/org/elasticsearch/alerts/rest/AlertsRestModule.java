/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest;

import org.elasticsearch.alerts.rest.action.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.rest.RestModule;

/**
 *
 */
public class AlertsRestModule extends AbstractModule implements PreProcessModule {

    @Override
    public void processModule(Module module) {
        if (module instanceof RestModule) {
            RestModule restModule = (RestModule) module;
            restModule.addRestAction(RestPutAlertAction.class);
            restModule.addRestAction(RestDeleteAlertAction.class);
            restModule.addRestAction(RestAlertsStatsAction.class);
            restModule.addRestAction(RestGetAlertAction.class);
            restModule.addRestAction(RestAlertServiceAction.class);
            restModule.addRestAction(RestAckAlertAction.class);
        }
    }

    @Override
    protected void configure() {
    }
}
