/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.omg.CORBA.NO_IMPLEMENT;

import java.util.Map;

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
        this.alertManager = alertManager;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel restChannel) throws Exception {
        logger.warn("GOT REST REQUEST");
        //@TODO : change these direct calls to actions/request/response/listener once we create the java client API
        if (request.method() == POST && request.path().contains("/_refresh")) {
            alertManager.refreshAlerts();
            restChannel.sendResponse(new BytesRestResponse(OK));
            return;
        } else if (request.method() == GET && request.path().contains("/_list")) {
            Map<String, Alert> alertMap = alertManager.getSafeAlertMap();
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            for (Map.Entry<String, Alert> alertEntry : alertMap.entrySet()) {
                builder.field(alertEntry.getKey());
                alertEntry.getValue().toXContent(builder);
            }
            builder.endObject();
            restChannel.sendResponse(new BytesRestResponse(OK,builder));
            return;
        } else if (request.method() == POST && request.path().contains("/_create")) {
            //TODO : this should all be moved to an action
            Alert alert = alertManager.parseAlert(request.param("name"), XContentHelper.convertToMap(request.content(), request.contentUnsafe()).v2());
            boolean added = alertManager.addAlert(alert.alertName(), alert, true);
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            alert.toXContent(builder);
            restChannel.sendResponse(new BytesRestResponse(OK,builder));
            return;
        } else if (request.method() == DELETE) {
            String alertName = request.param("name");
            alertManager.deleteAlert(alertName);
            restChannel.sendResponse(new BytesRestResponse(OK));
            return;
        }

        restChannel.sendResponse(new BytesRestResponse(NOT_IMPLEMENTED));
    }



}
