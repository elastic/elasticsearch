/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.rest.RestStatus.*;
import static org.elasticsearch.rest.RestRequest.Method.*;

public class AlertRestHandler implements RestHandler {

    ESLogger logger = Loggers.getLogger(AlertRestHandler.class);
    AlertManager alertManager;
    @Inject
    public AlertRestHandler(RestController restController, AlertManager alertManager) {
        restController.registerHandler(POST, "/_alerting/_refresh",this);
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
        if (request.method() == POST && request.path().contains("/_refresh")) {
            alertManager.refreshAlerts();
            XContentBuilder builder = getListOfAlerts();
            restChannel.sendResponse(new BytesRestResponse(OK,builder));
            return true;
        } else if (request.method() == GET && request.path().contains("/_list")) {
            XContentBuilder builder = getListOfAlerts();
            restChannel.sendResponse(new BytesRestResponse(OK,builder));
            return true;
        } else if (request.path().contains("/_enable")) {
            return alertManager.enableAlert(request.param("name"));
        } else if (request.path().contains("/_disable")) {
            return alertManager.disableAlert(request.param("name"));
        } else if (request.method() == POST && request.path().contains("/_create")) {
            //TODO : this should all be moved to an action
            Alert alert;
            try {
                alert = alertManager.parseAlert(request.param("name"), XContentHelper.convertToMap(request.content(), request.contentUnsafe()).v2());
            } catch (Exception e) {
                logger.error("Failed to parse alert", e);
                throw e;
            }
            try {
                boolean added = alertManager.addAlert(alert.alertName(), alert, true);
                if (added) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
                    alert.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    restChannel.sendResponse(new BytesRestResponse(OK, builder));
                } else {
                    restChannel.sendResponse(new BytesRestResponse(BAD_REQUEST));
                }
            } catch (ElasticsearchIllegalArgumentException eia) {
                XContentBuilder failed = XContentFactory.jsonBuilder().prettyPrint();
                failed.startObject();
                failed.field("ERROR", eia.getMessage());
                failed.endObject();
                restChannel.sendResponse(new BytesRestResponse(BAD_REQUEST,failed));
            }
            return true;
        } else if (request.method() == DELETE) {
            String alertName = request.param("name");
            logger.warn("Deleting [{}]", alertName);
            boolean successful = alertManager.deleteAlert(alertName);
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.field("Success", successful);
            builder.field("alertName", alertName);
            restChannel.sendResponse(new BytesRestResponse(OK));
            return true;
        }
        return false;
    }

    private XContentBuilder getListOfAlerts() throws IOException {
        Map<String, Alert> alertMap = alertManager.getSafeAlertMap();
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        for (Map.Entry<String, Alert> alertEntry : alertMap.entrySet()) {
            builder.field(alertEntry.getKey());
            alertEntry.getValue().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        return builder;
    }

}
