/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.test.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.IOException;

class RootHttpHandler implements HttpHandler {
    private static final Logger logger = ESLoggerFactory.getLogger(RootHttpHandler.class);

    @Override
    public void handle(HttpExchange t) throws IOException {
        logger.debug("Received ping call...");
        t.sendResponseHeaders(200, -1);
        t.close();
    }
}
