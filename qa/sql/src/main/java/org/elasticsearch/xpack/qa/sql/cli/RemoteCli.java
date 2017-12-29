/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.cli;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RemoteCli implements Closeable {
    private static final Logger logger = Loggers.getLogger(RemoteCli.class);

    private static final InetAddress CLI_FIXTURE_ADDRESS;
    private static final int CLI_FIXTURE_PORT;
    static {
        String addressAndPort = System.getProperty("tests.cli.fixture");
        if (addressAndPort == null) {
            throw new IllegalArgumentException("Must set the [tests.cli.fixture] property. Gradle handles this for you "
                    + " in regular tests. In embedded mode the easiest thing to do is run "
                    + "`gradle :x-pack-elasticsearch:qa:sql:no-security:run` and to set the property to the contents of "
                    + "`qa/sql/no-security/build/fixtures/cliFixture/ports`");
        }
        int split = addressAndPort.lastIndexOf(':');
        try {
            CLI_FIXTURE_ADDRESS = InetAddress.getByName(addressAndPort.substring(0, split));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        CLI_FIXTURE_PORT = Integer.parseInt(addressAndPort.substring(split + 1));
    }

    private final Socket socket;
    private final PrintWriter out;
    private final BufferedReader in;

    public RemoteCli(String elasticsearchAddress, boolean checkConnectionOnStartup,
            @Nullable SecurityConfig security) throws IOException {
        // Connect
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        logger.info("connecting to the cli fixture at {}:{}", CLI_FIXTURE_ADDRESS, CLI_FIXTURE_PORT);
        socket = AccessController.doPrivileged(new PrivilegedAction<Socket>() {
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
        socket.setSoTimeout(10000);
        out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

        // Start the CLI
        String command;
        if (security == null) {
            command = elasticsearchAddress;
        } else {
            command = security.user + "@" + elasticsearchAddress;
            if (security.https) {
                command = "https://" + command;
            } else if (randomBoolean()) {
                command = "http://" + command;
            }
            if (security.keystoreLocation != null) {
                command = command + " -keystore_location " + security.keystoreLocation;
            }
        }
        if (false == checkConnectionOnStartup) {
            command += " -check false";
        }
        /* Don't use println because it emits \r\n on windows but we put the
        * terminal in unix mode to make the tests consistent. */
        out.print(command + "\n");
        out.flush();

        // Feed it passwords if needed
        if (security != null && security.keystoreLocation != null) {
            assertEquals("keystore password: ", readUntil(s -> s.endsWith(": ")));
            out.print(security.keystorePassword + "\n");
            out.flush();
        }
        if (security != null) {
            assertEquals("password: ", readUntil(s -> s.endsWith(": ")));
            out.print(security.password + "\n");
            out.flush();
        }

        // Throw out the logo and warnings about making a dumb terminal
        while (false == readLine().contains("SQL"));
        // Throw out the empty line before all the good stuff
        assertEquals("", readLine());
    }

    /**
     * Attempts an orderly shutdown of the CLI, reporting any unconsumed lines as errors.
     */
    @Override
    public void close() throws IOException {
        try {
            // Try and shutdown the client normally
            /* Don't use println because it enits \r\n on windows but we put the
            * terminal in unix mode to make the tests consistent. */
            out.print("quit;\n");
            out.flush();
            List<String> nonQuit = new ArrayList<>();
            String line;
            while (false == (line = readLine()).startsWith("[?1h=[33msql> [0mquit;[90mBye![0m")) {
                if (false == line.isEmpty()) {
                    nonQuit.add(line);
                }
            }
            assertThat("unconsumed lines", nonQuit, empty());
        } finally {
            out.close();
            in.close();
            // Most importantly, close the socket so the next test can use the fixture
            socket.close();
        }
    }

    /**
     * Send a command and assert the echo.
     */
    public String command(String command) throws IOException {
        assertThat("; automatically added", command, not(endsWith(";")));
        logger.info("out: {};", command);
        /* Don't use println because it emits \r\n on windows but we put the
         * terminal in unix mode to make the tests consistent. */
        out.print(command + ";\n");
        out.flush();
        String firstResponse = "[?1h=[33msql> [0m" + command + ";";
        String firstLine = readLine();
        assertThat(firstLine, startsWith(firstResponse));
        return firstLine.substring(firstResponse.length());
    }

    public String readLine() throws IOException {
        /* Since we can't *see* esc in the error messages we just
         * remove it here and pretend it isn't required. Hopefully
         * `[` is enough for us to assert on. */
        String line = in.readLine().replace("\u001B", "");
        logger.info("in : {}", line);
        return line;
    }

    private String readUntil(Predicate<String> end) throws IOException {
        StringBuilder b = new StringBuilder();
        String result;
        do {
            int c = in.read();
            if (c == -1) {
                throw new IOException("got eof before end");
            }
            b.append((char) c);
            result = b.toString();
        } while (false == end.test(result));
        logger.info("in : {}", result);
        return result;
    }

    public static class SecurityConfig {
        private final boolean https;
        private final String user;
        private final String password;
        @Nullable
        private final String keystoreLocation;
        @Nullable
        private final String keystorePassword;

        public SecurityConfig(boolean https, String user, String password,
                @Nullable String keystoreLocation, @Nullable String keystorePassword) {
            if (user == null) {
                throw new IllegalArgumentException(
                    "[user] is required. Send [null] instead of a SecurityConfig to run without security.");
            }
            if (password == null) {
                throw new IllegalArgumentException(
                    "[password] is required. Send [null] instead of a SecurityConfig to run without security.");
            }
            if (keystoreLocation == null) {
                if (keystorePassword != null) {
                    throw new IllegalArgumentException("[keystorePassword] cannot be specified if [keystoreLocation] is not specified");
                }
            } else {
                if (keystorePassword == null) {
                    throw new IllegalArgumentException("[keystorePassword] is required if [keystoreLocation] is specified");
                }
            }

            this.https = https;
            this.user = user;
            this.password = password;
            this.keystoreLocation = keystoreLocation;
            this.keystorePassword = keystorePassword;
        }

        public String keystoreLocation() {
            return keystoreLocation;
        }

        public String keystorePassword() {
            return keystorePassword;
        }
    }
}
