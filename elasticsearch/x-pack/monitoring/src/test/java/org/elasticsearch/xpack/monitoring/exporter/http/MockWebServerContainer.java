/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.QueueDispatcher;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code MockWebServerContainer} wraps a {@link MockWebServer} to avoid forcing every usage of it to do the same thing.
 */
public class MockWebServerContainer implements AutoCloseable {

    private static Logger logger = Loggers.getLogger(MockWebServerContainer.class);

    /**
     * The running {@link MockWebServer}.
     */
    private final MockWebServer server;

    /**
     * Create a {@link MockWebServerContainer} that uses a port from [{@code 9250}, {code 9300}).
     *
     * @throws RuntimeException if an unrecoverable exception occurs (e.g., no open ports available)
     */
    public MockWebServerContainer() {
        this(9250, 9300);
    }

    /**
     * Create a {@link MockWebServerContainer} that uses a port from [{@code startPort}, {code 9300}).
     * <p>
     * This is useful if you need to test with two {@link MockWebServer}s, so you can simply skip the port of the existing one.
     *
     * @param startPort The first port to try (inclusive).
     * @throws RuntimeException if an unrecoverable exception occurs (e.g., no open ports available)
     */
    public MockWebServerContainer(final int startPort) {
        this(startPort, 9300);
    }

    /**
     * Create a {@link MockWebServerContainer} that uses a port from [{@code startPort}, {code endPort}).
     *
     * @param startPort The first port to try (inclusive).
     * @param endPort The last port to try (exclusive).
     * @throws RuntimeException if an unrecoverable exception occurs (e.g., no open ports available)
     */
    public MockWebServerContainer(final int startPort, final int endPort) {
        final List<Integer> failedPorts = new ArrayList<>(0);
        final QueueDispatcher dispatcher = new QueueDispatcher();
        dispatcher.setFailFast(true);

        MockWebServer webServer = null;

        for (int port = startPort; port < endPort; ++port) {
            try {
                webServer = new MockWebServer();
                webServer.setDispatcher(dispatcher);

                webServer.start(port);
                break;
            } catch (final BindException e) {
                failedPorts.add(port);
                webServer = null;
            } catch (final IOException e) {
                logger.error("unrecoverable failure while trying to start MockWebServer with port [{}]", e, port);
                throw new ElasticsearchException(e);
            }
        }

        if (webServer != null) {
            this.server = webServer;

            if (failedPorts.isEmpty() == false) {
                logger.warn("ports [{}] were already in use. using port [{}]", failedPorts, webServer.getPort());
            }
        } else {
            throw new ElasticsearchException("unable to find open port between [" + startPort + "] and [" + endPort + "]");
        }
    }

    /**
     * Get the {@link MockWebServer} created by this container.
     *
     * @return Never {@code null}.
     */
    public MockWebServer getWebServer() {
        return server;
    }

    /**
     * Get the port used by the running web server.
     *
     * @return The local port used by the {@linkplain #getWebServer() web server}.
     */
    public int getPort() {
        return server.getPort();
    }

    /**
     * Get the formatted address in the form of "hostname:port".
     *
     * @return Never {@code null}.
     */
    public String getFormattedAddress() {
        return server.getHostName() + ":" + server.getPort();
    }

    /**
     * Shutdown the {@linkplain #getWebServer() web server}.
     */
    @Override
    public void close() throws Exception {
        server.shutdown();
    }
}
