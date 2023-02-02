/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.Build;
import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ServerCliTests extends CommandTestCase {

    private SecureSettingsLoader mockSecureSettingsLoader;

    @Before
    public void setupMockConfig() throws IOException {
        Files.createFile(configDir.resolve("log4j2.properties"));
        mockSecureSettingsLoader = null;
    }

    @Override
    protected void assertUsage(Matcher<String> matcher, String... args) throws Exception {
        argsValidator = serverArgs -> fail("Should not have tried creating args on usage error");
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
        assertMutuallyExclusiveOptions("-V", "--enrollment-token", "mytoken");
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

    public void testMissingLoggingConfig() throws Exception {
        Files.delete(configDir.resolve("log4j2.properties"));
        int status = executeMain();
        assertThat(status, equalTo(ExitCodes.CONFIG));
        assertThat(terminal.getErrorOutput(), containsString("Missing logging config file"));
    }

    public void testPositionalArgs() throws Exception {
        String prefix = "Positional arguments not allowed, found ";
        assertUsage(containsString(prefix + "[foo]"), "foo");
        assertUsage(containsString(prefix + "[foo, bar]"), "foo", "bar");
        assertUsage(containsString(prefix + "[foo]"), "-E", "foo=bar", "foo", "-E", "baz=qux");
    }

    public void assertPidFile(String option) throws Exception {
        Path tmpDir = createTempDir();
        Path pidFileArg = tmpDir.resolve("pid");
        terminal.reset();
        argsValidator = args -> assertThat(args.pidFile().toString(), equalTo(pidFileArg.toString()));
        assertOk(option, pidFileArg.toString());
    }

    public void testPidFile() throws Exception {
        assertPidFile("-p");
        assertPidFile("--pidfile");

        assertUsage(containsString("Option p/pidfile requires an argument"), "-p");
        Path pidParentFile = createTempFile();
        assertUsage(containsString("exists but is not a directory"), "-p", pidParentFile.resolve("pid").toString());
        assertUsage(containsString("exists but is not a regular file"), "-p", createTempDir().toString());
    }

    public void testPidDirectories() throws Exception {
        Path tmpDir = createTempDir();

        Path pidFileArg = tmpDir.resolve("pid");
        argsValidator = args -> assertThat(args.pidFile().toString(), equalTo(pidFileArg.toString()));
        assertOk("-p", pidFileArg.toString());

        argsValidator = args -> assertThat(args.pidFile().toString(), equalTo(esHomeDir.resolve("pid").toAbsolutePath().toString()));
        assertOk("-p", "pid");
    }

    public void assertDaemonized(boolean daemonized, String... args) throws Exception {
        argsValidator = serverArgs -> assertThat(serverArgs.daemonize(), equalTo(daemonized));
        assertOk(args);
        assertThat(mockServer.detachCalled, is(daemonized));
        assertThat(mockServer.waitForCalled, not(equalTo(daemonized)));
    }

    public void testDaemonize() throws Exception {
        assertDaemonized(true, "-d");
        assertDaemonized(true, "--daemonize");
        assertDaemonized(false);
    }

    public void testQuiet() throws Exception {
        AtomicBoolean expectQuiet = new AtomicBoolean(true);
        argsValidator = args -> assertThat(args.quiet(), equalTo(expectQuiet.get()));
        assertOk("-q");
        assertOk("--quiet");
        expectQuiet.set(false);
        assertOk();
    }

    public void testElasticsearchSettings() throws Exception {
        argsValidator = args -> {
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
        argsValidator = args -> {
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

    public void testAutoConfigLogging() throws Exception {
        autoConfigCallback = (t, options, env, processInfo) -> {
            t.println("message from auto config");
            t.errorPrintln("error message");
            t.errorPrintln(Verbosity.VERBOSE, "verbose error");
        };
        assertOkWithOutput(
            containsString("message from auto config"),
            allOf(containsString("error message"), containsString("verbose error")),
            "-v"
        );
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

    public void testSyncPlugins() throws Exception {
        AtomicBoolean syncPluginsCalled = new AtomicBoolean(false);
        syncPluginsCallback = (t, options, env, processInfo) -> syncPluginsCalled.set(true);
        assertOk();
        assertThat(syncPluginsCalled.get(), is(true));
    }

    public void testSyncPluginsError() throws Exception {
        syncPluginsCallback = (t, options, env, processInfo) -> { throw new UserException(ExitCodes.CONFIG, "sync plugins failed"); };
        int gotMainExitCode = executeMain();
        assertThat(gotMainExitCode, equalTo(ExitCodes.CONFIG));
        assertThat(terminal.getErrorOutput(), containsString("sync plugins failed"));
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
        argsValidator = args -> assertThat(((KeyStoreWrapper) args.secrets()).hasPassword(), equalTo(hasPassword));
        autoConfigCallback = (t, options, env, processInfo) -> {
            char[] gotPassword = t.readSecret("");
            assertThat(gotPassword, equalTo(expectedPassword.toCharArray()));
        };
        assertOkWithOutput(emptyString(), hasPassword ? containsString("Enter password") : emptyString());
    }

    public void testKeystorePassword() throws Exception {
        assertKeystorePassword(null); // no keystore exists
        assertKeystorePassword("");
        assertKeystorePassword("a-dummy-password");
    }

    public void testCloseStopsServer() throws Exception {
        Command command = newCommand();
        command.main(new String[0], terminal, new ProcessInfo(sysprops, envVars, esHomeDir));
        command.close();
        assertThat(mockServer.stopCalled, is(true));
    }

    public void testIgnoreNullExceptionOutput() throws Exception {
        Command command = newCommand();

        autoConfigCallback = (t, options, env, processInfo) -> { throw new UserException(ExitCodes.NOOP, null); };
        terminal.reset();
        command.main(new String[0], terminal, new ProcessInfo(sysprops, envVars, esHomeDir));
        command.close();
        assertThat(terminal.getErrorOutput(), not(containsString("null")));
    }

    public void testServerExitsNonZero() throws Exception {
        mockServerExitCode = 140;
        int exitCode = executeMain();
        assertThat(exitCode, equalTo(140));
    }

    public void testSecureSettingsLoaderChoice() throws Exception {
        var loader = loadWithMockSecureSettingsLoader();
        assertTrue(loader.loaded);
        // the mock loader doesn't support autoconfigure, no need to bootstrap a keystore
        assertFalse(loader.bootstrapped);
        // assert that we ran the code to verify the environment
        assertTrue(loader.verifiedEnv);
    }

    public void testSecureSettingsLoaderWithPassword() throws Exception {
        var loader = setupMockKeystoreLoader();
        assertKeystorePassword("aaaaaaaaaaaaaaaaaa");
        assertTrue(loader.loaded);
        assertTrue(loader.bootstrapped);
        // the password we read should match what we passed in
        assertEquals("aaaaaaaaaaaaaaaaaa", loader.password);
        // after the command the secrets password is closed
        assertEquals(
            "SecureString has already been closed",
            expectThrows(IllegalStateException.class, () -> loader.secrets.password().get().getChars()).getMessage()
        );
    }

    public void testSecureSettingsLoaderWithEmptyPassword() throws Exception {
        var loader = setupMockKeystoreLoader();
        assertKeystorePassword("");
        assertTrue(loader.loaded);
        assertTrue(loader.bootstrapped);
        assertEquals("", loader.password);
    }

    public void testSecureSettingsLoaderWithNullPassword() throws Exception {
        var loader = setupMockKeystoreLoader();
        assertKeystorePassword(null); // no keystore exists
        assertTrue(loader.loaded);
        assertTrue(loader.bootstrapped);
        assertEquals("", loader.password);
    }

    private MockSecureSettingsLoader loadWithMockSecureSettingsLoader() throws Exception {
        var loader = new MockSecureSettingsLoader();
        this.mockSecureSettingsLoader = loader;
        Command command = newCommand();
        command.main(new String[0], terminal, new ProcessInfo(sysprops, envVars, esHomeDir));
        command.close();
        return loader;
    }

    private KeystoreSecureSettingsLoader setupMockKeystoreLoader() {
        var loader = new KeystoreSecureSettingsLoader();
        this.mockSecureSettingsLoader = loader;
        return loader;
    }

    interface AutoConfigMethod {
        void autoconfig(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws UserException;
    }

    Consumer<ServerArgs> argsValidator;
    private final MockServerProcess mockServer = new MockServerProcess();
    int mockServerExitCode = 0;

    AutoConfigMethod autoConfigCallback;
    private final MockAutoConfigCli AUTO_CONFIG_CLI = new MockAutoConfigCli();

    interface SyncPluginsMethod {
        void syncPlugins(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws UserException;
    }

    SyncPluginsMethod syncPluginsCallback;
    private final MockSyncPluginsCli SYNC_PLUGINS_CLI = new MockSyncPluginsCli();

    @Before
    public void resetCommand() {
        argsValidator = null;
        autoConfigCallback = null;
        syncPluginsCallback = null;
        mockServerExitCode = 0;
    }

    private class MockAutoConfigCli extends EnvironmentAwareCommand {
        private final OptionSpec<String> enrollmentTokenOption;

        MockAutoConfigCli() {
            super("mock auto config tool");
            enrollmentTokenOption = parser.accepts("enrollment-token").withRequiredArg();
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

    private class MockSyncPluginsCli extends EnvironmentAwareCommand {
        MockSyncPluginsCli() {
            super("mock sync plugins tool");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, ProcessInfo processInfo) throws Exception {
            fail("Called wrong execute method, must call the one that takes already parsed env");
        }

        @Override
        public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
            if (syncPluginsCallback != null) {
                syncPluginsCallback.syncPlugins(terminal, options, env, processInfo);
            }
        }
    }

    private class MockServerProcess extends ServerProcess {
        boolean detachCalled = false;
        boolean waitForCalled = false;
        boolean stopCalled = false;

        MockServerProcess() {
            super(null, null);
        }

        @Override
        public long pid() {
            return 12345;
        }

        @Override
        public void detach() {
            assert detachCalled == false;
            detachCalled = true;
        }

        @Override
        public int waitFor() {
            assert waitForCalled == false;
            waitForCalled = true;
            return mockServerExitCode;
        }

        @Override
        public void stop() {
            assert stopCalled == false;
            stopCalled = true;
        }

        void reset() {
            detachCalled = false;
            waitForCalled = false;
            stopCalled = false;
        }
    }

    @Override
    protected Command newCommand() {
        return new ServerCli() {
            @Override
            protected Command loadTool(String toolname, String libs) {
                if (toolname.equals("auto-configure-node")) {
                    assertThat(libs, equalTo("modules/x-pack-core,modules/x-pack-security,lib/tools/security-cli"));
                    return AUTO_CONFIG_CLI;
                } else if (toolname.equals("sync-plugins")) {
                    assertThat(libs, equalTo("lib/tools/plugin-cli"));
                    return SYNC_PLUGINS_CLI;
                }
                throw new AssertionError("Unknown tool: " + toolname);
            }

            @Override
            Environment autoConfigureSecurity(
                Terminal terminal,
                OptionSet options,
                ProcessInfo processInfo,
                Environment env,
                SecureString keystorePassword
            ) throws Exception {
                if (mockSecureSettingsLoader != null && mockSecureSettingsLoader.supportsSecurityAutoConfiguration() == false) {
                    fail("We shouldn't be calling auto configure on loaders that don't support it");
                }
                return super.autoConfigureSecurity(terminal, options, processInfo, env, keystorePassword);
            }

            @Override
            protected ServerProcess startServer(Terminal terminal, ProcessInfo processInfo, ServerArgs args) {
                if (argsValidator != null) {
                    argsValidator.accept(args);
                }
                mockServer.reset();
                return mockServer;
            }

            @Override
            void syncPlugins(Terminal terminal, Environment env, ProcessInfo processInfo) throws Exception {
                if (mockSecureSettingsLoader != null && mockSecureSettingsLoader instanceof MockSecureSettingsLoader mock) {
                    mock.verifiedEnv = true;
                    // equals as a pointer, environment shouldn't be changed if autoconfigure is not supported
                    assertFalse(mockSecureSettingsLoader.supportsSecurityAutoConfiguration());
                    assertTrue(mock.environment == env);
                }

                super.syncPlugins(terminal, env, processInfo);
            }

            @Override
            protected SecureSettingsLoader secureSettingsLoader(Environment env) {
                if (mockSecureSettingsLoader != null) {
                    return mockSecureSettingsLoader;
                }

                return new KeystoreSecureSettingsLoader();
            }
        };
    }

    static class MockSecureSettingsLoader implements SecureSettingsLoader {
        boolean loaded = false;
        LoadedSecrets secrets = null;
        String password = null;
        boolean bootstrapped = false;
        Environment environment = null;
        boolean verifiedEnv = false;

        @Override
        public SecureSettingsLoader.LoadedSecrets load(Environment environment, Terminal terminal) throws IOException {
            loaded = true;
            // Stash the environment pointer, so we can compare it. Environment shouldn't be changed for
            // loaders that don't autoconfigure.
            this.environment = environment;

            SecureString password = null;

            if (terminal.getReader().ready() == false) {
                this.password = null;
            } else {
                password = new SecureString(terminal.readSecret("Enter a password"));
                this.password = password.toString();
            }

            secrets = new SecureSettingsLoader.LoadedSecrets(
                KeyStoreWrapper.create(),
                password == null ? Optional.empty() : Optional.of(password)
            );

            return secrets;
        }

        @Override
        public SecureSettings bootstrap(Environment environment, SecureString password) throws Exception {
            fail("Bootstrap shouldn't be called for loaders that cannot be auto-configured");
            bootstrapped = true;
            return KeyStoreWrapper.create();
        }

        @Override
        public boolean supportsSecurityAutoConfiguration() {
            return false;
        }
    }

    static class KeystoreSecureSettingsLoader extends KeyStoreLoader {
        boolean loaded = false;
        LoadedSecrets secrets = null;
        String password = null;
        boolean bootstrapped = false;

        @Override
        public LoadedSecrets load(Environment environment, Terminal terminal) throws Exception {
            var result = super.load(environment, terminal);
            loaded = true;
            secrets = result;
            password = result.password().get().toString();

            return result;
        }

        @Override
        public SecureSettings bootstrap(Environment environment, SecureString password) throws Exception {
            this.bootstrapped = true;
            // make sure we don't fail in fips mode when we run with an empty password
            if (inFipsJvm() && (password == null || password.isEmpty())) {
                return KeyStoreWrapper.create();
            }
            return super.bootstrap(environment, password);
        }
    }
}
