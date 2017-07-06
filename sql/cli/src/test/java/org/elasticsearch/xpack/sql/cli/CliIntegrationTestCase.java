/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

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
import org.elasticsearch.xpack.sql.cli.CliIntegrationTestCase.CliThreadLeakFilter;
import org.jline.terminal.Attributes;
import org.jline.terminal.Attributes.LocalFlag;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.NonBlockingReader;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;

@ThreadLeakFilters(filters = CliThreadLeakFilter.class)
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
    public static final Supplier<CliConfiguration> ES = EMBED_SQL ? new EmbeddedCliServer() : () ->
            new CliConfiguration(System.getProperty("tests.rest.cluster") + "/_cli", new Properties());

    protected PrintWriter out;
    protected BufferedReader in;

    @Before
    public void startCli() throws IOException {
        PipedInputStream terminalIn = new PipedInputStream();
        out = new PrintWriter(new OutputStreamWriter(new PipedOutputStream(terminalIn), StandardCharsets.UTF_8), true);
        PipedOutputStream terminalOut = new PipedOutputStream();
        in = new BufferedReader(new InputStreamReader(new PipedInputStream(terminalOut), StandardCharsets.UTF_8));
        Terminal terminal = TerminalBuilder.builder()
                .system(false)
                .jansi(false)
                .jna(false)
                .type("dumb")
                .encoding("UTF-8")
                .streams(terminalIn, terminalOut)
                .build();
        // Work around https://github.com/jline/jline3/issues/145
        Attributes attrs = terminal.getAttributes();
        attrs.setLocalFlag(LocalFlag.ISIG, false);
        terminal.setAttributes(attrs);
        terminal.echo(false);
        Cli cli = new Cli(ES.get(), terminal) {
            @Override
            protected void handleExceptionWhileCommunicatingWithServer(PrintWriter out, RuntimeException e) {
                throw e;
            }
        };
        Thread cliThread = new Thread(() -> {
            try {
                cli.run();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        cliThread.start();

        // Throw out the logo so tests don't have to!
        while (false == in.readLine().contains("SQL"));
        // Throw out the empty line before all the good stuff
        assertEquals("", in.readLine());
    }

    @After
    public void shutdownCli() throws IOException, InterruptedException {
        try {
            // Try and quit
            out.println("quit;");
            List<String> nonQuit = new ArrayList<>();
            String line;
            while (false == (line = in.readLine()).equals("sql> quit;")) {
                nonQuit.add(line);
            }
            assertThat("unconsumed lines", nonQuit, empty());
            assertEquals("", in.readLine());
            assertEquals("Bye!", in.readLine());
            assertFalse(in.ready());
        } finally {
            // but close the streams anyway so the tests shut down cleanly
            out.close();
            in.close();
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
    protected void command(String command) throws IOException {
        assertThat("; automatically added", command, not(endsWith(";")));
        out.println(command + ";");
        assertEquals("sql> " + command + ";", in.readLine());
        assertEquals("", in.readLine());
    }

    /**
     * The {@link NonBlockingReader} just will not die. It looks harmless enough.
     * Hopefully it is ok to let it be.
     */
    public static class CliThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            for (StackTraceElement ste : t.getStackTrace()) {
                if (ste.getClassName().equals(NonBlockingReader.class.getName()) && ste.getMethodName().equals("run")) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Embedded CLI server that runs against a running Elasticsearch
     * server using the transport protocol.
     */
    private static class EmbeddedCliServer extends ExternalResource implements Supplier<CliConfiguration> {
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

        @Override
        public CliConfiguration get() {
            return new CliConfiguration(server.url(), new Properties());
        }
    }
}
