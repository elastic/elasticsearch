/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

@SuppressForbidden(reason = "use http server")
@SuppressWarnings("restriction")
class RootHandler implements HttpHandler {

    private static final Logger log = LogManager.getLogger(RootHandler.class.getName());

    @Override
    public void handle(HttpExchange http) throws IOException {
        log.debug("Received query call...");

        if ("HEAD".equals(http.getRequestMethod())) {
            http.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
            http.close();
            return;
        }

        fail(http, new UnsupportedOperationException("only HEAD allowed"));
    }

    private void fail(HttpExchange http, Exception ex) {
        log.error("Caught error while transmitting response", ex);
        try {
            // the error conversion has failed, halt
            if (http.getResponseHeaders().isEmpty()) {
                http.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
            }
        } catch (IOException ioEx) {
            log.error("Caught error while trying to catch error", ex);
        } finally {
            http.close();
        }
    }
}
