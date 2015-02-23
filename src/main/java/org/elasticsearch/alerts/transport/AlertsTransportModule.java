/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertAction;
import org.elasticsearch.alerts.transport.actions.ack.TransportAckAlertAction;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertAction;
import org.elasticsearch.alerts.transport.actions.delete.TransportDeleteAlertAction;
import org.elasticsearch.alerts.transport.actions.get.GetAlertAction;
import org.elasticsearch.alerts.transport.actions.get.TransportGetAlertAction;
import org.elasticsearch.alerts.transport.actions.put.PutAlertAction;
import org.elasticsearch.alerts.transport.actions.put.TransportPutAlertAction;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceAction;
import org.elasticsearch.alerts.transport.actions.service.TransportAlertsServiceAction;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsAction;
import org.elasticsearch.alerts.transport.actions.stats.TransportAlertsStatsAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;

/**
 *
 */
public class AlertsTransportModule extends AbstractModule implements PreProcessModule {

    @Override
    public void processModule(Module module) {
        if (module instanceof ActionModule) {
            ActionModule actionModule = (ActionModule) module;
            actionModule.registerAction(PutAlertAction.INSTANCE, TransportPutAlertAction.class);
            actionModule.registerAction(DeleteAlertAction.INSTANCE, TransportDeleteAlertAction.class);
            actionModule.registerAction(GetAlertAction.INSTANCE, TransportGetAlertAction.class);
            actionModule.registerAction(AlertsStatsAction.INSTANCE, TransportAlertsStatsAction.class);
            actionModule.registerAction(AckAlertAction.INSTANCE, TransportAckAlertAction.class);
            actionModule.registerAction(AlertsServiceAction.INSTANCE, TransportAlertsServiceAction.class);
        }
    }

    @Override
    protected void configure() {
    }

}
