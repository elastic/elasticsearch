/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.Build;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.nio.file.Path;
import java.util.Locale;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.allOf;

public class ElasticsearchCliTests extends ESElasticsearchCliTestCase {

    public void testVersion() throws Exception {
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("-V", "-d");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("-V", "--daemonize");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("-V", "-p", "/tmp/pid");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("-V", "--pidfile", "/tmp/pid");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("--version", "-d");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("--version", "--daemonize");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("--version", "-p", "/tmp/pid");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("--version", "--pidfile", "/tmp/pid");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("--version", "-q");
        runTestThatVersionIsMutuallyExclusiveToOtherOptions("--version", "--quiet");

        runTestThatVersionIsReturned("-V");
        runTestThatVersionIsReturned("--version");
    }

    private void runTestThatVersionIsMutuallyExclusiveToOtherOptions(String... args) throws Exception {
        runTestVersion(
                ExitCodes.USAGE,
                (output, error) -> assertThat(
                        error,
                        allOf(containsString("ERROR:"),
                              containsString("are unavailable given other options on the command line"))),
                args);
    }

    private void runTestThatVersionIsReturned(String... args) throws Exception {
        runTestVersion(ExitCodes.OK, (output, error) -> {
            assertThat(output, containsString("Version: " + Build.CURRENT.getQualifiedVersion()));
            final String expectedBuildOutput = String.format(
                    Locale.ROOT,
                    "Build: %s/%s/%s/%s",
                    Build.CURRENT.flavor().displayName(),
                    Build.CURRENT.type().displayName(),
                    Build.CURRENT.hash(),
                    Build.CURRENT.date());
            assertThat(output, containsString(expectedBuildOutput));
            assertThat(output, containsString("JVM: " + JvmInfo.jvmInfo().version()));
        }, args);
    }

    private void runTestVersion(int expectedStatus, BiConsumer<String, String> outputConsumer, String... args) throws Exception {
        runTest(expectedStatus, false, outputConsumer, (foreground, pidFile, quiet, esSettings) -> {}, args);
    }

    public void testPositionalArgs() throws Exception {
        runTest(
            ExitCodes.USAGE,
            false,
            (output, error) -> assertThat(error, containsString("Positional arguments not allowed, found [foo]")),
            (foreground, pidFile, quiet, esSettings) -> {},
            "foo");
        runTest(
            ExitCodes.USAGE,
            false,
            (output, error) -> assertThat(error, containsString("Positional arguments not allowed, found [foo, bar]")),
            (foreground, pidFile, quiet, esSettings) -> {},
            "foo", "bar");
        runTest(
            ExitCodes.USAGE,
            false,
            (output, error) -> assertThat(error, containsString("Positional arguments not allowed, found [foo]")),
            (foreground, pidFile, quiet, esSettings) -> {},
            "-E", "foo=bar", "foo", "-E", "baz=qux");
    }

    public void testThatPidFileCanBeConfigured() throws Exception {
        Path tmpDir = createTempDir();
        Path pidFile = tmpDir.resolve("pid");
        runPidFileTest(ExitCodes.USAGE, false,
            (output, error) -> assertThat(error, containsString("Option p/pidfile requires an argument")), pidFile, "-p");
        runPidFileTest(ExitCodes.OK, true, (output, error) -> {}, pidFile, "-p", pidFile.toString());
        runPidFileTest(ExitCodes.OK, true, (output, error) -> {}, pidFile, "--pidfile", tmpDir.toString() + "/pid");
    }

    private void runPidFileTest(final int expectedStatus, final boolean expectedInit, BiConsumer<String, String> outputConsumer,
                                Path expectedPidFile, final String... args)
            throws Exception {
        runTest(
                expectedStatus,
                expectedInit,
                outputConsumer,
                (foreground, pidFile, quiet, esSettings) -> assertThat(pidFile.toString(), equalTo(expectedPidFile.toString())),
                args);
    }

    public void testThatParsingDaemonizeWorks() throws Exception {
        runDaemonizeTest(true, "-d");
        runDaemonizeTest(true, "--daemonize");
        runDaemonizeTest(false);
    }

    private void runDaemonizeTest(final boolean expectedDaemonize, final String... args) throws Exception {
        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> assertThat(foreground, equalTo(!expectedDaemonize)),
                args);
    }

    public void testThatParsingQuietOptionWorks() throws Exception {
        runQuietTest(true, "-q");
        runQuietTest(true, "--quiet");
        runQuietTest(false);
    }

    private void runQuietTest(final boolean expectedQuiet, final String... args) throws Exception {
        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> assertThat(quiet, equalTo(expectedQuiet)),
                args);
    }

    public void testElasticsearchSettings() throws Exception {
        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, env) -> {
                    Settings settings = env.settings();
                    assertEquals("bar", settings.get("foo"));
                    assertEquals("qux", settings.get("baz"));
                },
                "-Efoo=bar", "-E", "baz=qux");
    }

    public void testElasticsearchSettingCanNotBeEmpty() throws Exception {
        runTest(
                ExitCodes.USAGE,
                false,
                (output, error) -> assertThat(error, containsString("setting [foo] must not be empty")),
                (foreground, pidFile, quiet, esSettings) -> {},
                "-E", "foo=");
    }

    public void testElasticsearchSettingCanNotBeDuplicated() throws Exception {
        runTest(
                ExitCodes.USAGE,
                false,
                (output, error) -> assertThat(error, containsString("setting [foo] already set, saw [bar] and [baz]")),
                (foreground, pidFile, quiet, initialEnv) -> {},
                "-E", "foo=bar", "-E", "foo=baz");
    }

    public void testUnknownOption() throws Exception {
        runTest(
                ExitCodes.USAGE,
                false,
                (output, error) -> assertThat(error, containsString("network.host is not a recognized option")),
                (foreground, pidFile, quiet, esSettings) -> {},
                "--network.host");
    }

}
