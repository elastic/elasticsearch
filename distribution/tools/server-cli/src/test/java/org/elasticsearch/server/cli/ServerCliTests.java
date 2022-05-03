/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import joptsimple.OptionSet;

import org.elasticsearch.Build;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasItem;

public class ServerCliTests extends CommandTestCase {

    private static final ExecutorService mockProcessExecutor = Executors.newSingleThreadExecutor();
    Path esConfigDir;

    @Before
    public void setupDummyInstallation() throws IOException {
        sysprops.put("java.home", "/javahome");
        esConfigDir = esHomeDir.resolve("config");
        Files.createDirectories(esConfigDir);
        Files.writeString(esConfigDir.resolve("jvm.options"), "");
    }

    @AfterClass
    public static void cleanupExecutor() {
        mockProcessExecutor.shutdown();
    }

    @Override
    protected void assertUsage(Matcher<String> matcher, String... args) throws Exception {
        mainCallback = FAIL_MAIN;
        super.assertUsage(matcher, args);
    }

    private void assertMutuallyExclusiveOptions(String... args) throws Exception {
        assertUsage(allOf(containsString("ERROR:"), containsString("are unavailable given other options on the command line")), args);
    }

    public void testVersion() throws Exception {
        assertMutuallyExclusiveOptions("-V", "-d");
        assertMutuallyExclusiveOptions("-V", "--daemonize");
        assertMutuallyExclusiveOptions("-V", "-p", "/tmp/pid");
        assertMutuallyExclusiveOptions("-V", "--pidfile", "/tmp/pid");
        assertMutuallyExclusiveOptions("--version", "-d");
        assertMutuallyExclusiveOptions("--version", "--daemonize");
        assertMutuallyExclusiveOptions("--version", "-p", "/tmp/pid");
        assertMutuallyExclusiveOptions("--version", "--pidfile", "/tmp/pid");
        assertMutuallyExclusiveOptions("--version", "-q");
        assertMutuallyExclusiveOptions("--version", "--quiet");

        final String expectedBuildOutput = String.format(
            Locale.ROOT,
            "Build: %s/%s/%s",
            Build.CURRENT.type().displayName(),
            Build.CURRENT.hash(),
            Build.CURRENT.date()
        );
        Matcher<String> versionOutput = allOf(
            containsString("Version: " + Build.CURRENT.qualifiedVersion()),
            containsString(expectedBuildOutput),
            containsString("JVM: " + JvmInfo.jvmInfo().version())
        );
        terminal.reset();
        assertOkWithOutput(versionOutput, emptyString(), "-V");
        terminal.reset();
        assertOkWithOutput(versionOutput, emptyString(), "--version");
    }

    public void testPositionalArgs() throws Exception {
        String prefix = "Positional arguments not allowed, found ";
        assertUsage(containsString(prefix + "[foo]"), "foo");
        assertUsage(containsString(prefix + "[foo, bar]"), "foo", "bar");
        assertUsage(containsString(prefix + "[foo]"), "-E", "foo=bar", "foo", "-E", "baz=qux");
    }

    public void testPidFile() throws Exception {
        Path tmpDir = createTempDir();
        Path pidFileArg = tmpDir.resolve("pid");
        assertUsage(containsString("Option p/pidfile requires an argument"), "-p");
        mainCallback = (args, stdout, stderr, exitCode) -> { assertThat(args.pidFile().toString(), equalTo(pidFileArg.toString())); };
        terminal.reset();
        assertOk("-p", pidFileArg.toString());
        terminal.reset();
        assertOk("--pidfile", pidFileArg.toString());
    }

    public void testDaemonize() throws Exception {
        AtomicBoolean expectDaemonize = new AtomicBoolean(true);
        mainCallback = (args, stdout, stderr, exitCode) -> assertThat(args.daemonize(), equalTo(expectDaemonize.get()));
        assertOk("-d");
        assertOk("--daemonize");
        expectDaemonize.set(false);
        assertOk();
    }

    public void testQuiet() throws Exception {
        AtomicBoolean expectQuiet = new AtomicBoolean(true);
        mainCallback = (args, stdout, stderr, exitCode) -> assertThat(args.quiet(), equalTo(expectQuiet.get()));
        assertOk("-q");
        assertOk("--quiet");
        expectQuiet.set(false);
        assertOk();
    }

    public void testElasticsearchSettings() throws Exception {
        mainCallback = (args, stdout, stderr, exitCode) -> {
            Settings settings = args.nodeSettings();
            assertThat(settings.get("foo"), equalTo("bar"));
            assertThat(settings.get("baz"), equalTo("qux"));
        };
        assertOk("-Efoo=bar", "-E", "baz=qux");
    }

    public void testElasticsearchSettingCanNotBeEmpty() throws Exception {
        assertUsage(containsString("setting [foo] must not be empty"), "-E", "foo=");
    }

    public void testElasticsearchSettingCanNotBeDuplicated() throws Exception {
        assertUsage(containsString("setting [foo] already set, saw [bar] and [baz]"), "-E", "foo=bar", "-E", "foo=baz");
    }

    public void testUnknownOption() throws Exception {
        assertUsage(containsString("network.host is not a recognized option"), "--network.host");
    }

    public void testPathHome() throws Exception {
        AtomicReference<String> expectedHomeDir = new AtomicReference<>();
        expectedHomeDir.set(esHomeDir.toString());
        mainCallback = (args, stdout, stderr, exitCode) -> {
            Settings settings = args.nodeSettings();
            assertThat(settings.get("path.home"), equalTo(expectedHomeDir.get()));
            assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
        };
        assertOk();
        sysprops.remove("es.path.home");
        final String commandLineValue = createTempDir().toString();
        expectedHomeDir.set(commandLineValue);
        assertOk("-Epath.home=" + commandLineValue);
    }

    public void testMissingEnrollmentToken() throws Exception {
        assertUsage(containsString("Option enrollment-token requires an argument"), "--enrollment-token");
    }

    public void testMultipleEnrollmentTokens() throws Exception {
        assertUsage(
            containsString("Multiple --enrollment-token parameters are not allowed"),
            "--enrollment-token",
            "some-token",
            "--enrollment-token",
            "some-other-token"
        );
    }

    public void testAutoConfigEnrollment() throws Exception {
        autoConfigCallback = (t, options, env, processInfo) -> {
            assertThat(options.valueOf("enrollment-token"), equalTo("mydummytoken"));
        };
        assertOk("--enrollment-token", "mydummytoken");
    }

    public void testAutoConfig() throws Exception {
        autoConfigCallback = (t, options, env, processInfo) -> { t.println("message from auto config"); };
        assertOkWithOutput(containsString("message from auto config"), emptyString());
    }

    public void assertAutoConfigError(int autoConfigExitCode, int expectedMainExitCode, String... args) throws Exception {
        terminal.reset();
        autoConfigCallback = (t, options, env, processInfo) -> { throw new UserException(autoConfigExitCode, "message from auto config"); };
        int gotMainExitCode = executeMain(args);
        assertThat(gotMainExitCode, equalTo(expectedMainExitCode));
        assertThat(terminal.getErrorOutput(), containsString("message from auto config"));
    }

    public void testAutoConfigErrorPropagated() throws Exception {
        assertAutoConfigError(ExitCodes.IO_ERROR, ExitCodes.IO_ERROR);
        terminal.reset();
        assertAutoConfigError(ExitCodes.CONFIG, ExitCodes.CONFIG, "--enrollment-token", "mytoken");
        terminal.reset();
        assertAutoConfigError(ExitCodes.DATA_ERROR, ExitCodes.DATA_ERROR, "--enrollment-token", "bogus");
    }

    public void testAutoConfigOkErrors() throws Exception {
        assertAutoConfigError(ExitCodes.CANT_CREATE, ExitCodes.OK);
        assertAutoConfigError(ExitCodes.CONFIG, ExitCodes.OK);
        assertAutoConfigError(ExitCodes.NOOP, ExitCodes.OK);
    }

    public void assertKeystorePassword(String password) throws Exception {
        terminal.reset();
        boolean hasPassword = password != null && password.isEmpty() == false;
        if (hasPassword) {
            terminal.addSecretInput(password);
        }
        Path configDir = esHomeDir.resolve("config");
        Files.createDirectories(configDir);
        if (hasPassword) {
            try (KeyStoreWrapper keystore = KeyStoreWrapper.create()) {
                keystore.save(configDir, password.toCharArray(), false);
            }
        }
        String expectedPassword = password == null ? "" : password;
        mainCallback = (args, stdout, stderr, exitCode) -> { assertThat(args.keystorePassword().toString(), equalTo(expectedPassword)); };
        autoConfigCallback = (t, options, env, processInfo) -> {
            char[] gotPassword = t.readSecret("");
            assertThat(gotPassword, equalTo(expectedPassword.toCharArray()));
        };
        assertOkWithOutput(emptyString(), hasPassword ? containsString("Enter password") : emptyString());
    }

    public void testKeystorePassword() throws Exception {
        assertKeystorePassword(null); // no keystore exists
        assertKeystorePassword("");
        assertKeystorePassword("dummypassword");
    }

    interface MainMethod {
        void main(ServerArgs args, OutputStream stdout, OutputStream stderr, AtomicInteger exitCode);
    }

    interface AutoConfigMethod {
        void autoconfig(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws UserException;
    }

    MainMethod mainCallback;
    final MainMethod FAIL_MAIN = (args, stdout, stderr, exitCode) -> fail("Did not expect to run init");

    AutoConfigMethod autoConfigCallback;
    private final MockAutoConfigCli AUTO_CONFIG_CLI = new MockAutoConfigCli();

    @Before
    public void resetCommand() {
        mainCallback = null;
        autoConfigCallback = null;
    }

    private class MockAutoConfigCli extends EnvironmentAwareCommand {
        MockAutoConfigCli() {
            super("mock auto config tool");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
            fail("Called wrong execute method, must call the one that takes already parsed env");
        }

        @Override
        public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
            // TODO: fake errors, check password from terminal, allow tests to make elasticsearch.yml change
            if (autoConfigCallback != null) {
                autoConfigCallback.autoconfig(terminal, options, env, processInfo);
            }
        }
    }

    // a "process" that is really another thread
    private class MockElasticsearchProcess extends Process {
        private final PipedOutputStream processStdin = new PipedOutputStream();
        private final PipedInputStream processStdout = new PipedInputStream();
        private final PipedInputStream processStderr = new PipedInputStream();
        private final PipedInputStream stdin = new PipedInputStream();
        private final PipedOutputStream stdout = new PipedOutputStream();
        private final PipedOutputStream stderr = new PipedOutputStream();

        private final AtomicInteger exitCode = new AtomicInteger();
        private final AtomicReference<IOException> argsParsingException = new AtomicReference<>();
        private final Future<?> main;

        MockElasticsearchProcess() throws IOException {
            stdin.connect(processStdin);
            stdout.connect(processStdout);
            stderr.connect(processStderr);
            this.main = mockProcessExecutor.submit(() -> {
                try (var in = new InputStreamStreamInput(stdin)) {
                    final ServerArgs serverArgs = new ServerArgs(in);
                    if (mainCallback != null) {
                        mainCallback.main(serverArgs, stdout, stderr, exitCode);
                    }
                } catch (IOException e) {
                    argsParsingException.set(e);
                }
                IOUtils.closeWhileHandlingException(stdin, stdout, stderr);
            });
        }

        @Override
        public OutputStream getOutputStream() {
            return processStdin;
        }

        @Override
        public InputStream getInputStream() {
            return processStdout;
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
            }
            if (argsParsingException.get() != null) {
                throw new AssertionError("Reading server args failed", argsParsingException.get());
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
            fail("Tried to kill ES process");
        }
    }

    @Override
    protected Command newCommand() {
        return new ServerCli() {

            @Override
            protected List<String> getJvmOptions(Path configDir, Path pluginsDir, Path tmpDir, String envOptions) throws Exception {
                return new ArrayList<>();
            }

            @Override
            protected Process startProcess(ProcessBuilder processBuilder) throws IOException {
                // TODO: validate processbuilder stuff
                return new MockElasticsearchProcess();
            }

            @Override
            protected Command loadTool(String toolname, String libs) {
                assertThat(toolname, equalTo("auto-configure-node"));
                assertThat(libs, equalTo("modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli"));
                return AUTO_CONFIG_CLI;
            }

            @Override
            public boolean addShutdownHook() {
                return false;
            }
        };
    }

}
