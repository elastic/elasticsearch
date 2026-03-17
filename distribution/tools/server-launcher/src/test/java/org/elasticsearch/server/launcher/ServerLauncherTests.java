/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher;

import org.elasticsearch.server.launcher.common.LaunchDescriptor;
import org.elasticsearch.server.launcher.common.ProcessUtil;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link ServerLauncher} and implicitly {@link ServerProcess}.
 * Uses an injectable process starter so no real subprocess is spawned.
 */
public class ServerLauncherTests extends ESTestCase {

    private static final ExecutorService MOCK_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final long MOCK_PID = 12345L;

    private ServerLauncher<LaunchDescriptor> launcher;
    private Path workingDir;
    private Path tempDir;
    private ByteArrayOutputStream capturedStderr;
    private PrintStream originalErr;
    private volatile ServerProcess activeServer;

    @FunctionalInterface
    private interface ServerBehavior {
        void run(InputStream stdin, OutputStream stderr, AtomicInteger exitCode) throws IOException;
    }

    /**
     * A mock Process that uses pipes for stdin/stderr and a background thread
     * that reads server args, writes the ready marker, then runs a configurable behavior.
     */
    private static class MockServerProcess extends Process {
        private final PipedOutputStream processStdin = new PipedOutputStream();
        private final PipedInputStream processStderr = new PipedInputStream();
        private final PipedInputStream stdin = new PipedInputStream();
        private final PipedOutputStream stderr = new PipedOutputStream();
        private final AtomicInteger exitCode = new AtomicInteger(0);
        private final Future<?> serverThread;

        MockServerProcess(LaunchDescriptor descriptor, ServerBehavior behavior) throws IOException {
            stdin.connect(processStdin);
            stderr.connect(processStderr);
            int argsLen = descriptor.serverArgsBytes().length;
            this.serverThread = MOCK_EXECUTOR.submit(() -> {
                try {
                    byte[] buf = new byte[Math.max(1, argsLen)];
                    int n = 0;
                    while (n < argsLen) {
                        int r = stdin.read(buf, n, argsLen - n);
                        if (r <= 0) break;
                        n += r;
                    }
                    try (PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                        err.println(ErrorPumpThread.SERVER_READY_MARKER);
                        behavior.run(stdin, stderr, exitCode);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    try {
                        stdin.close();
                        stderr.close();
                    } catch (IOException ignored) {}
                }
            });
        }

        @Override
        public OutputStream getOutputStream() {
            return processStdin;
        }

        @Override
        public InputStream getInputStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream getErrorStream() {
            return processStderr;
        }

        @Override
        public long pid() {
            return MOCK_PID;
        }

        @Override
        public int waitFor() throws InterruptedException {
            try {
                serverThread.get();
            } catch (ExecutionException e) {
                throw new AssertionError("Mock server thread failed", e.getCause());
            }
            return exitCode.get();
        }

        @Override
        public int exitValue() {
            if (serverThread.isDone() == false) {
                throw new IllegalThreadStateException();
            }
            return exitCode.get();
        }

        @Override
        public void destroy() {
            throw new UnsupportedOperationException("destroy not used in tests");
        }

        @Override
        public Process destroyForcibly() {
            serverThread.cancel(true);
            return this;
        }
    }

    @AfterClass
    public static void shutdownExecutor() {
        MOCK_EXECUTOR.shutdown();
    }

    @Before
    public void setUpTestDirs() throws Exception {
        launcher = new ServerLauncher<>();
        workingDir = createTempDir();
        tempDir = createTempDir();
        capturedStderr = new ByteArrayOutputStream();
        originalErr = System.err;
        System.setErr(new PrintStream(capturedStderr, true, StandardCharsets.UTF_8));
    }

    @Override
    public void tearDown() throws Exception {
        try {
            ServerProcess server = activeServer;
            if (server != null) {
                activeServer = null;
                server.stop();
            }
        } finally {
            try {
                System.setErr(originalErr);
            } finally {
                super.tearDown();
            }
        }
    }

    private LaunchDescriptor descriptor(
        String command,
        List<String> jvmOptions,
        List<String> jvmArgs,
        Map<String, String> environment,
        boolean daemonize,
        byte[] serverArgsBytes
    ) {
        return new LaunchDescriptor(
            command,
            jvmOptions != null ? jvmOptions : List.of(),
            jvmArgs != null ? jvmArgs : List.of(),
            environment != null ? environment : Map.of(),
            workingDir.toString(),
            tempDir.toString(),
            daemonize,
            serverArgsBytes != null ? serverArgsBytes : new byte[0]
        );
    }

    private String getCapturedStderr() {
        System.err.flush();
        return capturedStderr.toString(StandardCharsets.UTF_8);
    }

    private static MockServerProcess createMock(LaunchDescriptor d, ServerBehavior behavior) {
        try {
            return new MockServerProcess(d, behavior);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ServerProcess startServer(LaunchDescriptor d, Function<ProcessBuilder, Process> processStarter) throws Exception {
        ServerProcess server = launcher.startServer(d, processStarter);
        activeServer = server;
        return server;
    }

    public void testProcessBuilder() throws Exception {
        AtomicReference<ProcessBuilder> capturedPb = new AtomicReference<>();
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of(), false, new byte[0]);
        ServerBehavior behavior = (stdin, stderr, exitCode) -> {
            try (PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                err.println("stderr message");
            }
            stdin.read();
        };
        ServerProcess server = startServer(d, pb -> {
            capturedPb.set(pb);
            return createMock(d, behavior);
        });
        assertThat(capturedPb.get().redirectInput(), equalTo(ProcessBuilder.Redirect.PIPE));
        assertThat(capturedPb.get().redirectOutput(), equalTo(ProcessBuilder.Redirect.INHERIT));
        assertThat(capturedPb.get().redirectError(), equalTo(ProcessBuilder.Redirect.PIPE));
        assertThat(capturedPb.get().directory().toString(), equalTo(workingDir.toString()));
        server.stop();
        assertThat(getCapturedStderr(), containsString("stderr message"));
    }

    public void testPid() throws Exception {
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of(), true, new byte[0]);
        ServerBehavior behavior = (stdin, stderr, exitCode) -> stdin.read();
        ServerProcess server = startServer(d, pb -> createMock(d, behavior));
        assertThat(server.pid(), equalTo(MOCK_PID));
        server.stop();
    }

    public void testStartError() throws Exception {
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of(), false, new byte[0]);
        Exception e = expectThrows(UncheckedIOException.class, () -> startServer(d, pb -> {
            throw new UncheckedIOException(new IOException("something went wrong"));
        }));
        assertThat(e.getCause().getMessage(), equalTo("something went wrong"));
    }

    public void testEnvPassthrough() throws Exception {
        ProcessBuilder[] capturedPb = new ProcessBuilder[1];
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of("MY_ENV", "foo"), false, new byte[0]);
        ServerProcess server = startServer(d, pb -> {
            capturedPb[0] = pb;
            return createMock(d, (stdin, stderr, exitCode) -> stdin.read());
        });
        assertThat(capturedPb[0].environment(), hasEntry(equalTo("MY_ENV"), equalTo("foo")));
        server.stop();
    }

    public void testLibffiEnv() throws Exception {
        Path libffiDir = createTempDir();
        ProcessBuilder[] capturedPb = new ProcessBuilder[1];
        LaunchDescriptor d = descriptor(
            "/usr/bin/java",
            List.of(),
            List.of(),
            Map.of("LIBFFI_TMPDIR", libffiDir.toString()),
            false,
            new byte[0]
        );
        ServerProcess server = startServer(d, pb -> {
            capturedPb[0] = pb;
            return createMock(d, (stdin, stderr, exitCode) -> stdin.read());
        });
        assertThat(capturedPb[0].environment(), hasKey("LIBFFI_TMPDIR"));
        assertThat(Files.exists(Path.of(capturedPb[0].environment().get("LIBFFI_TMPDIR"))), is(true));
        server.stop();
    }

    public void testEnvCleared() throws Exception {
        ProcessBuilder[] capturedPb = new ProcessBuilder[1];
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of("SOME_VAR", "value"), false, new byte[0]);
        ServerProcess server = startServer(d, pb -> {
            capturedPb[0] = pb;
            return createMock(d, (stdin, stderr, exitCode) -> stdin.read());
        });
        assertThat(capturedPb[0].environment(), hasEntry(equalTo("SOME_VAR"), equalTo("value")));
        assertThat(capturedPb[0].environment(), not(hasKey("ES_TMPDIR")));
        assertThat(capturedPb[0].environment(), not(hasKey("ES_JAVA_OPTS")));
        server.stop();
    }

    public void testCommandLineSysprops() throws Exception {
        ProcessBuilder[] capturedPb = new ProcessBuilder[1];
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of("-Dfoo1=bar", "-Dfoo2=baz"), List.of(), Map.of(), false, new byte[0]);
        ServerProcess server = startServer(d, pb -> {
            capturedPb[0] = pb;
            return createMock(d, (stdin, stderr, exitCode) -> stdin.read());
        });
        assertThat(capturedPb[0].command(), hasItems("-Dfoo1=bar", "-Dfoo2=baz"));
        server.stop();
    }

    public void testCommandLine() throws Exception {
        String mainClass = "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch";
        String modulePath = workingDir.resolve("lib").toString();
        ProcessBuilder[] capturedPb = new ProcessBuilder[1];
        LaunchDescriptor d = descriptor(
            workingDir.resolve("bin/java").toString(),
            List.of(),
            List.of("--module-path", modulePath, "-m", mainClass),
            Map.of(),
            false,
            new byte[0]
        );
        ServerProcess server = startServer(d, pb -> {
            capturedPb[0] = pb;
            return createMock(d, (stdin, stderr, exitCode) -> stdin.read());
        });
        assertThat(capturedPb[0].command(), hasItems("--module-path", modulePath, "-m", mainClass));
        server.stop();
    }

    public void testDetach() throws Exception {
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of(), true, new byte[0]);
        AtomicReference<MockServerProcess> mockRef = new AtomicReference<>();
        ServerBehavior behavior = (stdin, stderr, exitCode) -> {
            try (PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                err.println("final message");
            }
            assertThat(stdin.read(), equalTo(-1));
        };
        ServerProcess server = startServer(d, pb -> {
            mockRef.set(createMock(d, behavior));
            return mockRef.get();
        });
        server.detach();
        assertThat(getCapturedStderr(), containsString("final message"));
        server.stop();
        ProcessUtil.nonInterruptible(() -> mockRef.get().waitFor());
    }

    public void testStop() throws Exception {
        CountDownLatch mainReady = new CountDownLatch(1);
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of(), false, new byte[0]);
        ServerBehavior behavior = (stdin, stderr, exitCode) -> {
            ProcessUtil.nonInterruptibleVoid(mainReady::await);
            try (PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                err.println("final message");
            }
            int b = stdin.read();
            assertThat(b, equalTo((int) ServerProcess.SERVER_SHUTDOWN_MARKER));
        };
        MockServerProcess mock = createMock(d, behavior);
        ServerProcess server = startServer(d, pb -> mock);
        mainReady.countDown();
        server.stop();
        assertThat(mock.serverThread.isDone(), is(true));
        assertThat(getCapturedStderr(), containsString("final message"));
    }

    public void testWaitFor() throws Exception {
        CountDownLatch mainReady = new CountDownLatch(1);
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of(), false, new byte[0]);
        ServerBehavior behavior = (stdin, stderr, exitCode) -> {
            mainReady.countDown();
            assertThat(stdin.read(), equalTo((int) ServerProcess.SERVER_SHUTDOWN_MARKER));
            try (PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                err.println("final message");
            }
        };
        MockServerProcess mock = createMock(d, behavior);
        ServerProcess server = startServer(d, pb -> mock);
        Thread stopThread = new Thread(() -> {
            ProcessUtil.nonInterruptibleVoid(mainReady::await);
            try {
                server.stop();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        stopThread.start();
        int exitCode = server.waitFor();
        assertThat(exitCode, equalTo(0));
        assertThat(mock.serverThread.isDone(), is(true));
        assertThat(getCapturedStderr(), containsString("final message"));
        stopThread.join();
    }

    public void testProcessDies() throws Exception {
        CountDownLatch mainExit = new CountDownLatch(1);
        LaunchDescriptor d = descriptor("/usr/bin/java", List.of(), List.of(), Map.of(), false, new byte[0]);
        ServerBehavior behavior = (stdin, stderr, exitCode) -> {
            try (PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                err.println("fatal message");
            }
            ProcessUtil.nonInterruptibleVoid(mainExit::await);
            exitCode.set(-9);
        };
        MockServerProcess mock = createMock(d, behavior);
        ServerProcess server = startServer(d, pb -> mock);
        mainExit.countDown();
        int exitCode = server.waitFor();
        assertThat(exitCode, equalTo(-9));
    }

    public void testLogsDirCreateParents() throws Exception {
        Path testDir = createTempDir();
        Path logsDir = testDir.resolve("subdir/logs");
        ProcessBuilder[] capturedPb = new ProcessBuilder[1];
        LaunchDescriptor d = new LaunchDescriptor(
            "/usr/bin/java",
            List.of(),
            List.of(),
            Map.of(),
            logsDir.toString(),
            tempDir.toString(),
            false,
            new byte[0]
        );
        ServerProcess server = startServer(d, pb -> {
            capturedPb[0] = pb;
            return createMock(d, (stdin, stderr, exitCode) -> stdin.read());
        });
        assertThat(capturedPb[0].directory().toString(), equalTo(logsDir.toString()));
        server.stop();
    }
}
