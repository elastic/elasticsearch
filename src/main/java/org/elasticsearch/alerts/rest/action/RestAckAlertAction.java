/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest.action;

import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertRequest;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

/**
 * The rest action to ack an alert
 */
public class RestAckAlertAction extends BaseRestHandler {

    private final AlertsClient alertsClient;

    @Inject
    protected RestAckAlertAction(Settings settings, RestController controller, Client client, AlertsClient alertsClient) {
        super(settings, controller, client);
        this.alertsClient = alertsClient;
        controller.registerHandler(RestRequest.Method.PUT, AlertsStore.ALERT_INDEX + "/alert/{name}/_ack", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel restChannel, Client client) throws Exception {
        final AckAlertRequest ackAlertRequest = new AckAlertRequest();
        ackAlertRequest.setAlertName(request.param("name"));
        alertsClient.ackAlert(ackAlertRequest, new RestBuilderListener<AckAlertResponse>(restChannel) {

            @Override
            public RestResponse buildResponse(AckAlertResponse ackAlertResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(AlertsStore.ACK_STATE_FIELD.getPreferredName(), ackAlertResponse.getAlertAckState().toString());
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
    
}
