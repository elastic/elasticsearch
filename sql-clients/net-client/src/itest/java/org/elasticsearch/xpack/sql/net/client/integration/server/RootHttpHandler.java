/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client.integration.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class RootHttpHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange t) throws IOException {
        System.out.println("Received ping call...");
        //String m = "SQL Proto testing server";
        //byte[] bytes = StringUtils.toUTF(m);
        t.sendResponseHeaders(200, -1);
        t.close();
    }
}
