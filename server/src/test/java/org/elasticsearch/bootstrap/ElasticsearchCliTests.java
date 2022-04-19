/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.Build;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasItem;

public class ElasticsearchCliTests extends CommandTestCase {
    private void assertOk(String... args) throws Exception {
        assertOkWithOutput(emptyString(), args);
    }

    private void assertOkWithOutput(Matcher<String> matcher, String... args) throws Exception {
        terminal.reset();
        int status = executeMain(args);
        assertThat(status, equalTo(ExitCodes.OK));
        assertThat(terminal.getErrorOutput(), emptyString());
        assertThat(terminal.getOutput(), matcher);
    }

    private void assertUsage(Matcher<String> matcher, String... args) throws Exception {
        terminal.reset();
        initCallback = FAIL_INIT;
        int status = executeMain(args);
        assertThat(status, equalTo(ExitCodes.USAGE));
        assertThat(terminal.getErrorOutput(), matcher);
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
        assertOkWithOutput(versionOutput, "-V");
        assertOkWithOutput(versionOutput, "--version");
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
        initCallback = (daemonize, pidFile, quiet, env) -> { assertThat(pidFile.toString(), equalTo(pidFileArg.toString())); };
        terminal.reset();
        assertOk("-p", pidFileArg.toString());
        terminal.reset();
        assertOk("--pidfile", pidFileArg.toString());
    }

    public void testDaemonize() throws Exception {
        AtomicBoolean expectDaemonize = new AtomicBoolean(true);
        initCallback = (d, p, q, e) -> assertThat(d, equalTo(expectDaemonize.get()));
        assertOk("-d");
        assertOk("--daemonize");
        expectDaemonize.set(false);
        assertOk();
    }

    public void testQuiet() throws Exception {
        AtomicBoolean expectQuiet = new AtomicBoolean(true);
        initCallback = (d, p, q, e) -> assertThat(q, equalTo(expectQuiet.get()));
        assertOk("-q");
        assertOk("--quiet");
        expectQuiet.set(false);
        assertOk();
    }

    public void testElasticsearchSettings() throws Exception {
        initCallback = (d, p, q, e) -> {
            Settings settings = e.settings();
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
        expectedHomeDir.set(homeDir.toString());
        initCallback = (d, p, q, e) -> {
            Settings settings = e.settings();
            assertThat(settings.get("path.home"), equalTo(expectedHomeDir.get()));
            assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
        };
        assertOk();
        homeDir = null;
        final String commandLineValue = createTempDir().toString();
        expectedHomeDir.set(commandLineValue);
        assertOk("-Epath.home=" + commandLineValue);
    }

    interface InitMethod {
        void init(boolean daemonize, Path pidFile, boolean quiet, Environment initialEnv);
    }

    Path homeDir;
    InitMethod initCallback;
    final InitMethod FAIL_INIT = (d, p, q, e) -> fail("Did not expect to run init");

    @Before
    public void resetCommand() {
        homeDir = createTempDir();
        initCallback = null;
    }

    @Override
    protected Command newCommand() {
        return new Elasticsearch() {
            @Override
            protected Map<String, String> captureSystemProperties() {
                if (homeDir == null) {
                    return Map.of("es.path.conf", createTempDir().toString());
                }
                return mockSystemProperties(homeDir);
            }

            @Override
            void init(boolean daemonize, Path pidFile, boolean quiet, Environment initialEnv) {
                if (initCallback != null) {
                    initCallback.init(daemonize, pidFile, quiet, initialEnv);
                }
            }

            @Override
            public boolean addShutdownHook() {
                return false;
            }
        };
    }
}
