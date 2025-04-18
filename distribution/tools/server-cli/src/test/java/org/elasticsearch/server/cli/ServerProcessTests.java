/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.Before;

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
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.bootstrap.BootstrapInfo.SERVER_READY_MARKER;
import static org.elasticsearch.server.cli.ProcessUtil.nonInterruptibleVoid;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ServerProcessTests extends ESTestCase {

    private static final ExecutorService mockJvmProcessExecutor = Executors.newSingleThreadExecutor();
    final MockTerminal terminal = MockTerminal.create();
    protected final Map<String, String> sysprops = new HashMap<>();
    protected final Map<String, String> envVars = new HashMap<>();
    Path esHomeDir;
    Path logsDir;
    Settings.Builder nodeSettings;
    ProcessValidator processValidator;
    MainMethod mainCallback;
    Runnable forceStopCallback;
    MockElasticsearchProcess process;
    SecureSettings secrets;

    interface MainMethod {
        void main(ServerArgs args, InputStream stdin, PrintStream stderr, AtomicInteger exitCode) throws IOException;
    }

    interface ProcessValidator {
        void validate(ProcessBuilder processBuilder) throws IOException;
    }

    int runForeground() throws Exception {
        var server = startProcess(false, false);
        return server.waitFor();
    }

    @Before
    public void resetEnv() {
        esHomeDir = createTempDir();
        terminal.reset();
        sysprops.clear();
        sysprops.put("os.name", "Linux");
        sysprops.put("java.home", "javahome");
        sysprops.put("es.path.home", esHomeDir.toString());
        logsDir = esHomeDir.resolve("logs");
        envVars.clear();
        nodeSettings = Settings.builder();
        processValidator = null;
        mainCallback = null;
        forceStopCallback = null;
        secrets = KeyStoreWrapper.create();
    }

    @AfterClass
    public static void cleanupExecutor() {
        mockJvmProcessExecutor.shutdown();
    }

    // a "process" that is really another thread
    private class MockElasticsearchProcess extends Process {
        private final PipedOutputStream processStdin = new PipedOutputStream();
        private final PipedInputStream processStderr = new PipedInputStream();
        private final PipedInputStream stdin = new PipedInputStream();
        private final PipedOutputStream stderr = new PipedOutputStream();

        private final AtomicInteger exitCode = new AtomicInteger();
        private final AtomicReference<IOException> processException = new AtomicReference<>();
        private final AtomicReference<AssertionError> assertion = new AtomicReference<>();
        private final Future<?> main;

        MockElasticsearchProcess() throws IOException {
            stdin.connect(processStdin);
            stderr.connect(processStderr);
            this.main = mockJvmProcessExecutor.submit(() -> {
                var in = new InputStreamStreamInput(stdin);
                try {
                    var serverArgs = new ServerArgs(in);
                    try (var err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                        if (mainCallback != null) {
                            mainCallback.main(serverArgs, stdin, err, exitCode);
                        } else {
                            err.println(SERVER_READY_MARKER);
                        }
                    }
                } catch (IOException e) {
                    processException.set(e);
                } catch (AssertionError e) {
                    assertion.set(e);
                }
                IOUtils.closeWhileHandlingException(stdin, stderr);
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
            return 12345;
        }

        @Override
        public int waitFor() throws InterruptedException {
            try {
                main.get();
            } catch (ExecutionException e) {
                throw new AssertionError(e);
            } catch (CancellationException e) {
                return 137; // process killed
            }
            if (processException.get() != null) {
                throw new AssertionError("Process failed", processException.get());
            }
            if (assertion.get() != null) {
                throw assertion.get();
            }
            return exitCode.get();
        }

        @Override
        public int exitValue() {
            if (main.isDone() == false) {
                throw new IllegalThreadStateException(); // match spec
            }
            return exitCode.get();
        }

        @Override
        public void destroy() {
            fail("Tried to kill ES process directly");
        }

        public Process destroyForcibly() {
            main.cancel(true);
            IOUtils.closeWhileHandlingException(stdin, stderr);
            forceStopCallback.run();
            return this;
        }
    }

    ProcessInfo createProcessInfo() {
        return new ProcessInfo(Map.copyOf(sysprops), Map.copyOf(envVars), esHomeDir);
    }

    ServerArgs createServerArgs(boolean daemonize, boolean quiet) {
        return new ServerArgs(daemonize, quiet, null, secrets, nodeSettings.build(), esHomeDir.resolve("config"), logsDir);
    }

    ServerProcess startProcess(boolean daemonize, boolean quiet) throws Exception {
        var pinfo = createProcessInfo();
        ServerProcessBuilder.ProcessStarter starter = pb -> {
            if (processValidator != null) {
                processValidator.validate(pb);
            }
            process = new MockElasticsearchProcess();
            return process;
        };
        var serverProcessBuilder = new ServerProcessBuilder().withTerminal(terminal)
            .withProcessInfo(pinfo)
            .withServerArgs(createServerArgs(daemonize, quiet))
            .withJvmOptions(List.of())
            .withTempDir(ServerProcessUtils.setupTempDir(pinfo));
        return serverProcessBuilder.start(starter);
    }

    public void testProcessBuilder() throws Exception {
        processValidator = pb -> {
            assertThat(pb.redirectInput(), equalTo(ProcessBuilder.Redirect.PIPE));
            assertThat(pb.redirectOutput(), equalTo(ProcessBuilder.Redirect.INHERIT));
            assertThat(pb.redirectError(), equalTo(ProcessBuilder.Redirect.PIPE));
            assertThat(String.valueOf(pb.directory()), equalTo(esHomeDir.resolve("logs").toString()));
        };
        mainCallback = (args, stdin, stderr, exitCode) -> {
            try (PrintStream err = new PrintStream(stderr, true, StandardCharsets.UTF_8)) {
                err.println("stderr message");
                err.println(SERVER_READY_MARKER);
            }
        };
        runForeground();
        assertThat(terminal.getErrorOutput(), containsString("stderr message"));
    }

    public void testPid() throws Exception {
        var server = startProcess(true, false);
        assertThat(server.pid(), equalTo(12345L));
        server.stop();
    }

    public void testBootstrapError() throws Exception {
        mainCallback = (args, stdin, stderr, exitCode) -> {
            stderr.println("a bootstrap exception");
            exitCode.set(ExitCodes.CONFIG);
        };
        var e = expectThrows(UserException.class, this::runForeground);
        assertThat(e.exitCode, equalTo(ExitCodes.CONFIG));
        assertThat(terminal.getErrorOutput(), containsString("a bootstrap exception"));
    }

    public void testStartError() {
        processValidator = pb -> { throw new IOException("something went wrong"); };
        var e = expectThrows(UncheckedIOException.class, this::runForeground);
        assertThat(e.getCause().getMessage(), equalTo("something went wrong"));
    }

    public void testEnvPassthrough() throws Exception {
        envVars.put("MY_ENV", "foo");
        processValidator = pb -> { assertThat(pb.environment(), hasEntry(equalTo("MY_ENV"), equalTo("foo"))); };
        runForeground();
    }

    public void testLibffiEnv() throws Exception {
        processValidator = pb -> {
            assertThat(pb.environment(), hasKey("LIBFFI_TMPDIR"));
            Path libffi = Paths.get(pb.environment().get("LIBFFI_TMPDIR"));
            assertThat(Files.exists(libffi), is(true));
        };
        runForeground();
        envVars.put("LIBFFI_TMPDIR", "mylibffi_tmp");
        processValidator = pb -> { assertThat(pb.environment(), hasEntry(equalTo("LIBFFI_TMPDIR"), equalTo("mylibffi_tmp"))); };
        runForeground();
    }

    public void testEnvCleared() throws Exception {
        Path customTmpDir = createTempDir();
        envVars.put("ES_TMPDIR", customTmpDir.toString());
        envVars.put("ES_JAVA_OPTS", "-Dmyoption=foo");

        processValidator = pb -> {
            assertThat(pb.environment(), not(hasKey("ES_TMPDIR")));
            assertThat(pb.environment(), not(hasKey("ES_JAVA_OPTS")));
        };
        runForeground();
    }

    public void testCommandLineSysprops() throws Exception {
        ServerProcessBuilder.ProcessStarter starter = pb -> {
            assertThat(pb.command(), hasItems("-Dfoo1=bar", "-Dfoo2=baz"));
            process = new MockElasticsearchProcess();
            return process;
        };
        var serverProcessBuilder = new ServerProcessBuilder().withTerminal(terminal)
            .withProcessInfo(createProcessInfo())
            .withServerArgs(createServerArgs(false, false))
            .withJvmOptions(List.of("-Dfoo1=bar", "-Dfoo2=baz"))
            .withTempDir(Path.of("."));
        serverProcessBuilder.start(starter).waitFor();
    }

    public void testServerProcessBuilderMissingArgumentError() throws Exception {
        ServerProcessBuilder.ProcessStarter starter = pb -> new MockElasticsearchProcess();
        var serverProcessBuilder = new ServerProcessBuilder().withTerminal(terminal)
            .withProcessInfo(createProcessInfo())
            .withServerArgs(createServerArgs(false, false))
            .withTempDir(Path.of("."));
        var ex = expectThrows(IllegalStateException.class, () -> serverProcessBuilder.start(starter).waitFor());
        assertThat(ex.getMessage(), equalTo("'jvmOptions' is a required argument and needs to be specified before calling start()"));
    }

    public void testCommandLine() throws Exception {
        String mainClass = "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch";
        String modulePath = esHomeDir.resolve("lib").toString();
        Path javaBin = Paths.get("javahome").resolve("bin");
        AtomicReference<String> expectedJava = new AtomicReference<>(javaBin.resolve("java").toString());
        processValidator = pb -> { assertThat(pb.command(), hasItems(expectedJava.get(), "--module-path", modulePath, "-m", mainClass)); };
        runForeground();

        sysprops.put("os.name", "Windows 10");
        sysprops.put("java.io.tmpdir", createTempDir().toString());
        expectedJava.set(javaBin.resolve("java.exe").toString());
        runForeground();
    }

    public void testDetach() throws Exception {
        mainCallback = (args, stdin, stderr, exitCode) -> {
            assertThat(args.daemonize(), equalTo(true));
            stderr.println(SERVER_READY_MARKER);
            stderr.println("final message");
            stderr.close();
            // will block until stdin closed manually after test
            assertThat(stdin.read(), equalTo(-1));
        };
        var server = startProcess(true, false);
        server.detach();
        assertThat(terminal.getErrorOutput(), containsString("final message"));
        server.stop(); // this should be a noop, and will fail the stdin read assert above if shutdown sent
        process.processStdin.close(); // unblock the "process" thread so it can exit
    }

    public void testStop() throws Exception {
        CountDownLatch mainReady = new CountDownLatch(1);
        mainCallback = (args, stdin, stderr, exitCode) -> {
            stderr.println(SERVER_READY_MARKER);
            nonInterruptibleVoid(mainReady::await);
            stderr.println("final message");
        };
        var server = startProcess(false, false);
        mainReady.countDown();
        server.stop();
        assertThat(process.main.isDone(), is(true)); // stop should have waited
        assertThat(terminal.getErrorOutput(), containsString("final message"));
    }

    public void testForceStop() throws Exception {
        CountDownLatch blockMain = new CountDownLatch(1);
        CountDownLatch inMain = new CountDownLatch(1);
        mainCallback = (args, stdin, stderr, exitCode) -> {
            stderr.println(SERVER_READY_MARKER);
            inMain.countDown();
            nonInterruptibleVoid(blockMain::await);
        };
        var server = startProcess(false, false);
        nonInterruptibleVoid(inMain::await);
        forceStopCallback = blockMain::countDown;
        server.forceStop();

        assertThat(process.main.isCancelled(), is(true)); // stop should have waited
    }

    public void testWaitFor() throws Exception {
        CountDownLatch mainReady = new CountDownLatch(1);
        mainCallback = (args, stdin, stderr, exitCode) -> {
            stderr.println(SERVER_READY_MARKER);
            mainReady.countDown();
            assertThat(stdin.read(), equalTo((int) BootstrapInfo.SERVER_SHUTDOWN_MARKER));
            stderr.println("final message");
        };
        var server = startProcess(false, false);

        CompletableFuture<Void> stopping = new CompletableFuture<>();
        new Thread(() -> {
            try {
                // simulate stop run as shutdown hook in another thread, eg from Ctrl-C
                nonInterruptibleVoid(mainReady::await);
                server.stop();
                stopping.complete(null);
            } catch (Throwable e) {
                stopping.completeExceptionally(e);
            }
        }).start();
        int exitCode = server.waitFor();
        assertThat(process.main.isDone(), is(true));
        assertThat(exitCode, equalTo(0));
        assertThat(terminal.getErrorOutput(), containsString("final message"));
        // rethrow any potential exception observed while stopping
        stopping.get();
    }

    public void testProcessDies() throws Exception {
        CountDownLatch mainExit = new CountDownLatch(1);
        mainCallback = (args, stdin, stderr, exitCode) -> {
            stderr.println(SERVER_READY_MARKER);
            stderr.println("fatal message");
            stderr.close(); // mimic pipe break if cli process dies
            nonInterruptibleVoid(mainExit::await);
            exitCode.set(-9);
        };
        var server = startProcess(false, false);
        mainExit.countDown();
        int exitCode = server.waitFor();
        assertThat(exitCode, equalTo(-9));
    }

    public void testLogsDirIsFile() throws Exception {
        Files.createFile(logsDir);
        var e = expectThrows(UserException.class, this::runForeground);
        assertThat(e.getMessage(), containsString("exists but is not a directory"));
    }

    public void testLogsDirCreateParents() throws Exception {
        Path testDir = createTempDir();
        logsDir = testDir.resolve("subdir/logs");
        processValidator = pb -> assertThat(String.valueOf(pb.directory()), equalTo(logsDir.toString()));
        runForeground();
    }

    public void testLogsCreateFailure() throws Exception {
        Path testDir = createTempDir();
        Path parentFile = testDir.resolve("exists");
        Files.createFile(parentFile);
        logsDir = parentFile.resolve("logs");
        var e = expectThrows(UserException.class, this::runForeground);
        assertThat(e.getMessage(), containsString("Unable to create logs dir"));
    }
}
