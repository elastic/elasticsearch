/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.cli;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.qa.sql.embed.CliHttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessControlException;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;

public abstract class CliIntegrationTestCase extends ESRestTestCase {
    /**
     * Should the HTTP server that serves SQL be embedded in the test
     * process (true) or should the JDBC driver connect to Elasticsearch
     * running at {@code tests.rest.cluster}. Note that to use embedded
     * HTTP you have to have Elasticsearch's transport protocol open on
     * port 9300 but the Elasticsearch running there does not need to have
     * the SQL plugin installed. Note also that embedded HTTP is faster
     * but is not canonical because it runs against a different HTTP server
     * then JDBC will use in production. Gradle always uses non-embedded.
     */
    private static final boolean EMBED_SQL = Booleans.parseBoolean(System.getProperty("tests.embed.sql", "false"));

    @ClassRule
    public static final EmbeddedCliServer EMBEDDED = EMBED_SQL ? new EmbeddedCliServer() : null;
    public static final Supplier<String> ES = EMBED_SQL ? EMBEDDED::address : CliIntegrationTestCase::elasticsearchAddress;

    /**
     * Read an address for Elasticsearch suitable for the CLI from the system properties.
     */
    public static String elasticsearchAddress() {
        String cluster = System.getProperty("tests.rest.cluster");
        // CLI only supports a single node at a time so we just give it one.
        return cluster.split(",")[0];
    }

    private RemoteCli cli;

    /**
     * Asks the CLI Fixture to start a CLI instance.
     */
    @Before
    public void startCli() throws IOException {
        cli = new RemoteCli(esUrlPrefix() + ES.get());
    }

    @After
    public void orderlyShutdown() throws IOException, InterruptedException {
        if (cli == null) {
            // failed to connect to the cli so there is nothing to do here
            return;
        }
        cli.close();
    }

    /**
     * Prefix to the Elasticsearch URL. Override to add
     * authentication support.
     */
    protected String esUrlPrefix() {
        return "";
    }

    protected void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        HttpEntity doc = new StringEntity(builder.string(), ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + index + "/doc/1", singletonMap("refresh", "true"), doc);
    }

    public String command(String command) throws IOException {
        return cli.command(command);
    }

    public String readLine() throws IOException {
        return cli.readLine();
    }

    /**
     * Embedded CLI server that runs against a running Elasticsearch
     * server using the transport protocol.
     */
    private static class EmbeddedCliServer extends ExternalResource {
        private Client client;
        private CliHttpServer server;

        @Override
        @SuppressWarnings("resource")
        protected void before() throws Throwable {
            try {
                Settings settings = Settings.builder()
                        .put("client.transport.ignore_cluster_name", true)
                        .build();
                client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getLoopbackAddress(), 9300));
            } catch (ExceptionInInitializerError e) {
                if (e.getCause() instanceof AccessControlException) {
                    throw new RuntimeException(getClass().getSimpleName() + " is not available with the security manager", e);
                } else {
                    throw e;
                }
            }
            server = new CliHttpServer(client);

            server.start(0);
        }

        @Override
        protected void after() {
            client.close();
            client = null;
            server.stop();
            server = null;
        }

        private String address() {
            return server.address().getAddress().getHostAddress() + ":" + server.address().getPort();
        }
    }
}
