/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest.action;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestDeleteAlertAction extends BaseRestHandler {

    private final AlertsClient alertsClient;

    @Inject
    public RestDeleteAlertAction(Settings settings, RestController controller, Client client, AlertsClient alertsClient) {
        super(settings, controller, client);
        this.alertsClient = alertsClient;
        controller.registerHandler(DELETE, AlertsStore.ALERT_INDEX + "/alert/{name}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        DeleteAlertRequest indexAlertRequest = new DeleteAlertRequest();
        indexAlertRequest.setAlertName(request.param("name"));
        alertsClient.deleteAlert(indexAlertRequest, new RestBuilderListener<DeleteAlertResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteAlertResponse result, XContentBuilder builder) throws Exception {
                DeleteResponse deleteResponse = result.deleteResponse();
                builder.startObject()
                        .field("found", deleteResponse.isFound())
                        .field("_index", deleteResponse.getIndex())
                        .field("_type", deleteResponse.getType())
                        .field("_id", deleteResponse.getId())
                        .field("_version", deleteResponse.getVersion())
                        .endObject();
                RestStatus status = OK;
                if (!deleteResponse.isFound()) {
                    status = NOT_FOUND;
                }
                return new BytesRestResponse(status, builder);
            }
        });
    }
}
