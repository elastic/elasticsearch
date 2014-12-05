/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.client.NodeAlertsClient;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.rest.*;
import org.elasticsearch.alerts.scheduler.AlertScheduler;
import org.elasticsearch.alerts.transport.actions.ack.TransportAckAlertAction;
import org.elasticsearch.alerts.transport.actions.config.TransportConfigAlertAction;
import org.elasticsearch.alerts.transport.actions.delete.TransportDeleteAlertAction;
import org.elasticsearch.alerts.transport.actions.get.TransportGetAlertAction;
import org.elasticsearch.alerts.transport.actions.put.TransportPutAlertAction;
import org.elasticsearch.alerts.transport.actions.service.TransportAlertsServiceAction;
import org.elasticsearch.alerts.transport.actions.stats.TransportAlertStatsAction;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.common.inject.AbstractModule;


public class AlertingModule extends AbstractModule {

    @Override
    protected void configure() {
        // Core components
        bind(TemplateHelper.class).asEagerSingleton();
        bind(AlertsStore.class).asEagerSingleton();
        bind(AlertManager.class).asEagerSingleton();
        bind(AlertActionManager.class).asEagerSingleton();
        bind(TriggerManager.class).asEagerSingleton();
        bind(AlertScheduler.class).asEagerSingleton();
        bind(AlertActionRegistry.class).asEagerSingleton();
        bind(ConfigurationManager.class).asEagerSingleton();

        // Transport and client layer
        bind(TransportPutAlertAction.class).asEagerSingleton();
        bind(TransportDeleteAlertAction.class).asEagerSingleton();
        bind(TransportGetAlertAction.class).asEagerSingleton();
        bind(TransportAlertStatsAction.class).asEagerSingleton();
        bind(TransportAckAlertAction.class).asEagerSingleton();
        bind(TransportAlertsServiceAction.class).asEagerSingleton();
        bind(TransportConfigAlertAction.class).asEagerSingleton();
        bind(AlertsClient.class).to(NodeAlertsClient.class).asEagerSingleton();

        // Rest layer
        bind(RestPutAlertAction.class).asEagerSingleton();
        bind(RestDeleteAlertAction.class).asEagerSingleton();
        bind(RestAlertsStatsAction.class).asEagerSingleton();
        bind(RestGetAlertAction.class).asEagerSingleton();
        bind(RestAlertServiceAction.class).asEagerSingleton();
        bind(RestConfigAlertAction.class).asEagerSingleton();
    }

}
