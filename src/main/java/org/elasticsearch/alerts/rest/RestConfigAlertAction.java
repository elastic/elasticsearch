/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.ConfigurationManager;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.transport.actions.config.ConfigAlertRequest;
import org.elasticsearch.alerts.transport.actions.config.ConfigAlertResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class RestConfigAlertAction extends BaseRestHandler {

    private final AlertsClient alertsClient;

    @Inject
    protected RestConfigAlertAction(Settings settings, RestController controller, Client client, AlertsClient alertsClient) {
        super(settings, controller, client);
        this.alertsClient = alertsClient;
        String path = AlertsStore.ALERT_INDEX + "/" + ConfigurationManager.CONFIG_TYPE + "/" + ConfigurationManager.GLOBAL_CONFIG_NAME;
        controller.registerHandler(RestRequest.Method.PUT, path, this);
        controller.registerHandler(RestRequest.Method.POST, path, this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        ConfigAlertRequest configAlertRequest = new ConfigAlertRequest();
        configAlertRequest.setConfigSource(request.content());
        configAlertRequest.setConfigSourceUnsafe(request.contentUnsafe());
        alertsClient.alertConfig(configAlertRequest, new RestBuilderListener<ConfigAlertResponse>(channel) {
            @Override
            public RestResponse buildResponse(ConfigAlertResponse response, XContentBuilder builder) throws Exception {
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