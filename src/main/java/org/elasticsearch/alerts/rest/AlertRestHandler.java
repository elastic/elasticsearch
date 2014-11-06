/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.rest;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;

public class AlertRestHandler implements RestHandler {

    ESLogger logger = Loggers.getLogger(AlertRestHandler.class);
    AlertManager alertManager;
    @Inject
    public AlertRestHandler(RestController restController, AlertManager alertManager) {
        restController.registerHandler(POST, "/_alerting/_refresh",this);
        restController.registerHandler(GET, "/_alerting/_refresh",this);
        restController.registerHandler(GET, "/_alerting/_list",this);
        restController.registerHandler(POST, "/_alerting/_create/{name}", this);
        restController.registerHandler(DELETE, "/_alerting/_delete/{name}", this);
        restController.registerHandler(GET, "/_alerting/_enable/{name}", this);
        restController.registerHandler(GET, "/_alerting/_disable/{name}", this);
        restController.registerHandler(POST, "/_alerting/_enable/{name}", this);
        restController.registerHandler(POST, "/_alerting/_disable/{name}", this);

        this.alertManager = alertManager;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel restChannel) throws Exception {
        try {
            if (dispatchRequest(request, restChannel)) {
                return;
            }
        } catch (Throwable t){
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.field("error", t.getMessage());
            builder.field("stack", t.getStackTrace());
            builder.endObject();
            restChannel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR, builder));
        }
        restChannel.sendResponse(new BytesRestResponse(NOT_IMPLEMENTED));
    }

    private boolean dispatchRequest(RestRequest request, RestChannel restChannel) throws IOException, InterruptedException, ExecutionException {
        //@TODO : change these direct calls to actions/request/response/listener once we create the java client API
        if (request.path().contains("/_refresh")) {
            XContentBuilder builder = getListOfAlerts();
            restChannel.sendResponse(new BytesRestResponse(OK,builder));
            return true;
        } else if (request.method() == GET && request.path().contains("/_list")) {
            XContentBuilder builder = getListOfAlerts();
            restChannel.sendResponse(new BytesRestResponse(OK,builder));
            return true;
        } else if (request.path().contains("/_enable")) {
            logger.warn("Enabling [{}]", request.param("name"));
            String alertName = request.param("name");
            boolean enabled = true;//alertManager.enableAlert(alertName);
            XContentBuilder responseBuilder = buildEnabledResponse(alertName, enabled);
            restChannel.sendResponse(new BytesRestResponse(OK,responseBuilder));
            return true;
        } else if (request.path().contains("/_disable")) {
            logger.warn("Disabling [{}]", request.param("name"));
            String alertName = request.param("name");
            boolean enabled = true;//alertManager.disableAlert(alertName);
            XContentBuilder responseBuilder = buildEnabledResponse(alertName, enabled);
            restChannel.sendResponse(new BytesRestResponse(OK,responseBuilder));
            return true;
        } else if (request.method() == POST && request.path().contains("/_create")) {
            IndexResponse response = alertManager.addAlert(request.param("name"), request.content());
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject()
                    .field("_index", response.getIndex())
                    .field("_type", response.getType())
                    .field("_id", response.getId())
                    .field("_version", response.getVersion())
                    .field("created", response.isCreated());
            builder.endObject();
            RestStatus status = OK;
            if (response.isCreated()) {
                status = CREATED;
            }
            restChannel.sendResponse(new BytesRestResponse(status, builder));
            return true;
        } else if (request.method() == DELETE) {
            String alertName = request.param("name");
            logger.warn("Deleting [{}]", alertName);
            DeleteResponse deleteResponse = alertManager.deleteAlert(alertName);
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.field("Success", deleteResponse != null);
            builder.field("alertName", alertName);
            restChannel.sendResponse(new BytesRestResponse(OK));
            return true;
        }
        return false;
    }

    private XContentBuilder buildEnabledResponse(String alertName, boolean enabled) throws IOException {
        XContentBuilder responseBuilder = XContentFactory.jsonBuilder().prettyPrint();
        responseBuilder.startObject();
        responseBuilder.field(alertName);
        responseBuilder.startObject();
        responseBuilder.field("enabled",enabled);
        responseBuilder.endObject();
        responseBuilder.endObject();
        return responseBuilder;
    }

    private XContentBuilder getListOfAlerts() throws IOException {
        List<Alert> alerts = alertManager.getAllAlerts();
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        for (Alert alert : alerts) {
            builder.field(alert.alertName());
            alert.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        return builder;
    }

}
