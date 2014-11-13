/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequest;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The rest action to get an alert
 */
public class RestGetAlertAction extends BaseRestHandler {

    private final AlertsClient alertsClient;

    @Inject
    public RestGetAlertAction(Settings settings, RestController controller, Client client, AlertsClient alertsClient) {
        super(settings, controller, client);
        this.alertsClient = alertsClient;
        controller.registerHandler(GET, "/_alert/{name}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        GetAlertRequest getAlertRequest = new GetAlertRequest();
        getAlertRequest.alertName(request.param("name"));
        alertsClient.getAlert(getAlertRequest, new RestBuilderListener<GetAlertResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAlertResponse result, XContentBuilder builder) throws Exception {
                GetResponse getResponse = result.getResponse();
                getResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
                RestStatus status = OK;
                if (!getResponse.isExists()) {
                    status = NOT_FOUND;
                }
                return new BytesRestResponse(status, builder);
            }
        });
    }
}