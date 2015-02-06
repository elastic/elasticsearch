/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest.action;

import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceRequest;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

/**
 */
public class RestAlertServiceAction extends BaseRestHandler {

    private final AlertsClient alertsClient;

    @Inject
    protected RestAlertServiceAction(Settings settings, RestController controller, Client client, AlertsClient alertsClient) {
        super(settings, controller, client);
        this.alertsClient = alertsClient;
        controller.registerHandler(RestRequest.Method.PUT, AlertsStore.ALERT_INDEX + "/_restart", this);
        controller.registerHandler(RestRequest.Method.PUT, AlertsStore.ALERT_INDEX + "/_start", new StartRestHandler(settings, controller, client));
        controller.registerHandler(RestRequest.Method.PUT, AlertsStore.ALERT_INDEX + "/_stop", new StopRestHandler(settings, controller, client));
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        AlertsServiceRequest serviceRequest = new AlertsServiceRequest();
        serviceRequest.restart();
        alertsClient.alertService(serviceRequest, new AcknowledgedRestListener<AlertsServiceResponse>(channel));
    }

    final class StartRestHandler extends BaseRestHandler {

        public StartRestHandler(Settings settings, RestController controller, Client client) {
            super(settings, controller, client);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
            AlertsServiceRequest serviceRequest = new AlertsServiceRequest();
            serviceRequest.start();
            alertsClient.alertService(serviceRequest, new AcknowledgedRestListener<AlertsServiceResponse>(channel));
        }
    }

    final class StopRestHandler extends BaseRestHandler {

        public StopRestHandler(Settings settings, RestController controller, Client client) {
            super(settings, controller, client);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
            AlertsServiceRequest serviceRequest = new AlertsServiceRequest();
            serviceRequest.stop();
            alertsClient.alertService(serviceRequest, new AcknowledgedRestListener<AlertsServiceResponse>(channel));
        }
    }
}
