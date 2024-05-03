/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.azure;

import com.sun.net.httpserver.HttpServer;

import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class AzureHttpFixture extends ExternalResource {

    private final boolean enabled;
    private final String account;
    private final String container;
    private HttpServer server;

    public AzureHttpFixture(boolean enabled, String account, String container) {
        this.enabled = enabled;
        this.account = account;
        this.container = container;
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/" + account;
    }

    @Override
    protected void before() throws IOException {
        if (enabled) {
            this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/" + account, new AzureHttpHandler(account, container));
            server.start();
        }
    }

    @Override
    protected void after() {
        if (enabled) {
            server.stop(0);
        }
    }
}
