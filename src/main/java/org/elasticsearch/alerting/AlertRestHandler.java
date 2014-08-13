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
import org.elasticsearch.rest.*;

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
        if (request.method() == POST && request.path().contains("/_refresh") ) {
            alertManager.refreshAlerts();
        }
        restChannel.sendResponse(new BytesRestResponse(OK));
    }
}
