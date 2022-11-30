/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.sql.cli.Cli;
import org.elasticsearch.xpack.sql.cli.CliTerminal;
import org.elasticsearch.xpack.sql.cli.JLineTerminal;
import org.jline.terminal.impl.ExternalTerminal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Wraps a CLI in as "real" a way as it can get without forking the CLI
 * subprocess with the goal being integration testing of the CLI without
 * breaking our security model by forking. We test the script that starts
 * the CLI using packaging tests which is super "real" but not super fast
 * and doesn't run super frequently.
 */
public class EmbeddedCli implements Closeable {
    private static final Logger logger = LogManager.getLogger(EmbeddedCli.class);

    private final Thread exec;
    private final Cli cli;
    private final AtomicInteger returnCode = new AtomicInteger(Integer.MIN_VALUE);
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final BufferedWriter out;
    private final BufferedReader in;
    /**
     * Has the client already been closed?
     */
    private boolean closed = false;

    public EmbeddedCli(String elasticsearchAddress, boolean checkConnectionOnStartup, @Nullable SecurityConfig security)
        throws IOException {
        PipedOutputStream outgoing = new PipedOutputStream();
        PipedInputStream cliIn = new PipedInputStream(outgoing);
        PipedInputStream incoming = new PipedInputStream();
        PipedOutputStream cliOut = new PipedOutputStream(incoming);
        CliTerminal cliTerminal = new JLineTerminal(
            new ExternalTerminal("test", "xterm-256color", cliIn, cliOut, StandardCharsets.UTF_8),
            false
        );
        cli = new Cli(cliTerminal) {
        };
        out = new BufferedWriter(new OutputStreamWriter(outgoing, StandardCharsets.UTF_8));
        in = new BufferedReader(new InputStreamReader(incoming, StandardCharsets.UTF_8));

        List<String> args = new ArrayList<>();
        if (security == null) {
            args.add(elasticsearchAddress);
        } else {
            String address = security.user + "@" + elasticsearchAddress;
            if (security.https) {
                address = "https://" + address;
            } else if (randomBoolean()) {
                address = "http://" + address;
            }
            args.add(address);
            if (security.keystoreLocation != null) {
                args.add("-keystore_location");
                args.add(security.keystoreLocation);
            }
        }
        if (false == checkConnectionOnStartup) {
            args.add("-check");
            args.add("false");
        }
        args.add("-debug");

        if (randomBoolean()) {
            args.add("-binary");
            args.add(Boolean.toString(randomBoolean()));
        }

        exec = new Thread(() -> {
            try {
                /*
                 * We don't really interact with the terminal because we're
                 * trying to test our interaction with jLine which doesn't
                 * support Elasticsearch's Terminal abstraction.
                 */
                Terminal terminal = MockTerminal.create();
                int exitCode = cli.main(args.toArray(new String[0]), terminal, new ProcessInfo(Map.of(), Map.of(), createTempDir()));
                returnCode.set(exitCode);
                logger.info("cli exited with code [{}]", exitCode);
            } catch (Exception e) {
                failure.set(e);
            }
        });
        exec.start();

        try {
            // Feed it passwords if needed
            if (security != null) {
                String passwordPrompt = "[?1h=[?2004hpassword: ";
                if (security.keystoreLocation != null) {
                    assertEquals("[?1h=[?2004hkeystore password: ", readUntil(s -> s.endsWith(": ")));
                    out.write(security.keystorePassword + "\n");
                    out.flush();
                    logger.info("out: {}", security.keystorePassword);
                    // Read the newline echoed after the password prompt
                    assertEquals("", readLine());
                    /*
                     * And for some reason jLine adds a second one so
                     * consume that too. I'm not sure why it does this
                     * but it looks right when a use runs the cli.
                     */
                    assertEquals("", readLine());
                    /*
                     * If we read the keystore password the console will
                     * emit some state reset escape sequences before the
                     * prompt for the password.
                     */
                    passwordPrompt = "[?1l>[?1000l[?2004l[?1h=[?2004hpassword: ";
                }
                assertEquals(passwordPrompt, readUntil(s -> s.endsWith(": ")));
                out.write(security.password + "\n");
                out.flush();
                logger.info("out: {}", security.password);
                // Read the newline echoed after the password prompt
                assertEquals("", readLine());
            }

            // Read until the first "good" line (skip the logo or read until an exception)
            boolean isLogoOrException = false;
            while (isLogoOrException == false) {
                String line = readLine();
                if ("SQL".equals(line.trim())) {
                    // it's almost the bottom of the logo, so read the next line (the version) and break out of the loop
                    readLine();
                    isLogoOrException = true;
                } else if (line.contains("Exception")) {
                    // if it's an exception, just break out of the loop and don't read the next line
                    // as it will swallow the exception and IT tests won't catch it
                    isLogoOrException = true;
                }
            }

            assertConnectionTest();
        } catch (IOException e) {
            try {
                forceClose();
            } catch (Exception closeException) {
                e.addSuppressed(closeException);
                throw e;
            }
        }
    }

    /**
     * Assert that result of the connection test. Default implementation
     * asserts that the test passes but overridden to check places where
     * we want to assert that it fails.
     */
    protected void assertConnectionTest() throws IOException {
        // After the connection test passess we emit an empty line and then the prompt
        assertEquals("", readLine());
    }

    /**
     * Attempts an orderly shutdown of the CLI, reporting any unconsumed lines as errors.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            // Try and shutdown the client normally

            /*
             * Don't use command here because we want want
             * to collect all the responses and report them
             * as failures if there is a problem rather than
             * failing on the first bad response.
             */
            out.write("quit;\n");
            out.flush();
            List<String> nonQuit = new ArrayList<>();
            String line;
            while (true) {
                line = readLine();
                if (line == null) {
                    fail("got EOF before [Bye!]. Extras " + nonQuit);
                }
                if (line.contains("quit;")) {
                    continue;
                }
                if (line.contains("Bye!")) {
                    break;
                }
                if (false == line.isEmpty()) {
                    nonQuit.add(line);
                }
            }
            assertThat("unconsumed lines", nonQuit, empty());
        } finally {
            forceClose();
        }
        assertEquals(0, returnCode.get());
    }

    /**
     * Shutdown the connection to the remote CLI without attempting to shut
     * the remote down in an orderly way.
     */
    public void forceClose() throws IOException {
        closed = true;
        IOUtils.close(out, in, cli);
        try {
            exec.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        Exception e = failure.get();
        if (e != null) {
            throw new RuntimeException("CLI thread failed", e);
        }
    }

    /**
     * Send a command and assert the echo.
     */
    public String command(String command) throws IOException {
        assertThat("; automatically added", command, not(endsWith(";")));
        logger.info("out: {};", command);
        out.write(command + ";\n");
        out.flush();
        for (String echo : expectedCommandEchos(command)) {
            assertEquals(echo, readLine());
        }
        return readLine();
    }

    /**
     * Create the "echo" that we expect jLine to send to the terminal
     * while we're typing a command.
     */
    private List<String> expectedCommandEchos(String command) {
        List<String> commandLines = Arrays.stream(command.split("\n")).filter(s -> s.isEmpty() == false).toList();
        List<String> result = new ArrayList<>(commandLines.size() * 2);
        result.add("[?1h=[?2004h[33msql> [0m" + commandLines.get(0));
        // Every line gets an extra new line because, I dunno, but it looks right in the CLI
        result.add("");
        for (int i = 1; i < commandLines.size(); i++) {
            result.add("[?1l>[?1000l[?2004l[?1h=[?2004h[33m   | [0m" + commandLines.get(i));
            // Every line gets an extra new line because, I dunno, but it looks right in the CLI
            result.add("");
        }
        result.set(result.size() - 2, result.get(result.size() - 2) + ";");
        return result;
    }

    public String readLine() throws IOException {
        /*
         * Since we can't *see* esc in the error messages we just
         * remove it here and pretend it isn't required. Hopefully
         * `[` is enough for us to assert on.
         *
         * `null` means EOF so we should just pass that back through.
         */
        String line = in.readLine();
        line = line == null ? null : line.replace("\u001B", "");
        logger.info("in : {}", line);
        return line;
    }

    private String readUntil(Predicate<String> end) throws IOException {
        StringBuilder b = new StringBuilder();
        String result;
        while (true) {
            int c = in.read();
            if (c == -1) {
                throw new IOException("got eof before end");
            }
            if (c == '\u001B') {
                /*
                 * Since we can't *see* esc in the error messages we just
                 * remove it here and pretend it isn't required. Hopefully
                 * `[` is enough for us to assert on.
                 */
                continue;
            }
            b.append((char) c);
            result = b.toString();
            if (end.test(result)) {
                break;
            }
        }
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

        public SecurityConfig(
            boolean https,
            String user,
            String password,
            @Nullable String keystoreLocation,
            @Nullable String keystorePassword
        ) {
            if (user == null) {
                throw new IllegalArgumentException("[user] is required. Send [null] instead of a SecurityConfig to run without security.");
            }
            if (password == null) {
                throw new IllegalArgumentException(
                    "[password] is required. Send [null] instead of a SecurityConfig to run without security."
                );
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
