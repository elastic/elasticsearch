/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest.action;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.put.PutAlertRequest;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestPutAlertAction extends BaseRestHandler {

    private final AlertsClient alertsClient;

    @Inject
    public RestPutAlertAction(Settings settings, RestController controller, Client client, AlertsClient alertsClient) {
        super(settings, controller, client);
        this.alertsClient = alertsClient;
        controller.registerHandler(POST, AlertsStore.ALERT_INDEX + "/alert/{name}", this);
        controller.registerHandler(PUT, AlertsStore.ALERT_INDEX + "/alert/{name}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        PutAlertRequest putAlertRequest = new PutAlertRequest();
        putAlertRequest.setAlertName(request.param("name"));
        putAlertRequest.setAlertSource(request.content(), request.contentUnsafe());
        alertsClient.putAlert(putAlertRequest, new RestBuilderListener<PutAlertResponse>(channel) {
            @Override
            public RestResponse buildResponse(PutAlertResponse response, XContentBuilder builder) throws Exception {
                IndexResponse indexResponse = response.indexResponse();
                builder.startObject()
                        .field("_index", indexResponse.getIndex())
                        .field("_type", indexResponse.getType())
                        .field("_id", indexResponse.getId())
                        .field("_version", indexResponse.getVersion())
                        .field("created", indexResponse.isCreated());
                builder.endObject();
                RestStatus status = OK;
                if (indexResponse.isCreated()) {
                    status = CREATED;
                }
                return new BytesRestResponse(status, builder);
            }
        });
    }
}
