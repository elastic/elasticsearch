/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.cli;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.SpecialPermission;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public abstract class CliIntegrationTestCase extends ESRestTestCase {
    private static final InetAddress CLI_FIXTURE_ADDRESS;
    private static final int CLI_FIXTURE_PORT;
    static {
        String addressAndPort = System.getProperty("tests.cli.fixture");
        if (addressAndPort == null) {
            throw new IllegalArgumentException("Must set the [tests.cli.fixture] property. This is required even in embedded "
                    + "mode. The easiest way to run the fixture is with "
                    + "`gradle :x-pack-elasticsearch:test:sql-cli-fixture:run` and to set the property to localhost:53798");
        }
        int split = addressAndPort.lastIndexOf(':');
        try {
            CLI_FIXTURE_ADDRESS = InetAddress.getByName(addressAndPort.substring(0, split));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        CLI_FIXTURE_PORT = Integer.parseInt(addressAndPort.substring(split + 1));
    }

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

    private Socket cliSocket;
    private PrintWriter out;
    private BufferedReader in;

    /**
     * Asks the CLI Fixture to start a CLI instance.
     */
    @Before
    public void startCli() throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        logger.info("connecting to the cli fixture at {}:{}", CLI_FIXTURE_ADDRESS, CLI_FIXTURE_PORT);
        cliSocket = AccessController.doPrivileged(new PrivilegedAction<Socket>() {
            @Override
            public Socket run() {
                try {
                    return new Socket(CLI_FIXTURE_ADDRESS, CLI_FIXTURE_PORT);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        logger.info("connected");

        out = new PrintWriter(new OutputStreamWriter(cliSocket.getOutputStream(), StandardCharsets.UTF_8), true);
        out.println(ES.get());
        in = new BufferedReader(new InputStreamReader(cliSocket.getInputStream(), StandardCharsets.UTF_8));
        // Throw out the logo and warnings about making a dumb terminal
        while (false == readLine().contains("SQL"));
        // Throw out the empty line before all the good stuff
        assertEquals("", readLine());
    }

    @After
    public void orderlyShutdown() throws IOException, InterruptedException {
        if (cliSocket == null) {
            // failed to connect to the cli so there is nothing to do here
            return;
        }
        try {
            // Try and shutdown the client normally
            out.println("quit;");
            List<String> nonQuit = new ArrayList<>();
            String line;
            while (false == (line = readLine()).equals("[?1h=[33msql> [0mquit;[90mBye![0m")) {
                if (false == line.isEmpty()) {
                    nonQuit.add(line);
                }
            }
            assertThat("unconsumed lines", nonQuit, empty());
        } finally {
            out.close();
            in.close();
            // Most importantly, close the socket so the next test can use the fixture
            cliSocket.close();
        }
    }

    protected void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        HttpEntity doc = new StringEntity(builder.string(), ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + index + "/doc/1", singletonMap("refresh", "true"), doc);
    }

    /**
     * Send a command and assert the echo.
     */
    protected String command(String command) throws IOException {
        assertThat("; automatically added", command, not(endsWith(";")));
        logger.debug("out: {};", command);
        out.println(command + ";");
        String firstResponse = "[?1h=[33msql> [0m" + command + ";";
        String firstLine = readLine();
        assertThat(firstLine, startsWith(firstResponse));
        return firstLine.substring(firstResponse.length());
    }

    protected String readLine() throws IOException {
        /* Since we can't *see* esc in the error messages we just
         * remove it here and pretend it isn't required. Hopefully
         * `[` is enough for us to assert on. */
        String line = in.readLine().replace("\u001B", "");
        logger.debug("in : {}", line);
        return line;
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

    private static String elasticsearchAddress() {
        String cluster = System.getProperty("tests.rest.cluster");
        // CLI only supports a single node at a time so we just give it one.
        return cluster.split(",")[0];
    }
}
