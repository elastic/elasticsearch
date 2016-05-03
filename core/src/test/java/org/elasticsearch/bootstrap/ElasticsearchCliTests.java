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
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

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
        runTestThatVersionIsReturned("-V");
        runTestThatVersionIsReturned("--version");
    }

    private void runTestThatVersionIsMutuallyExclusiveToOtherOptions(String... args) throws Exception {
        runTestVersion(
                ExitCodes.USAGE,
                output -> assertThat(
                        output,
                        containsString("ERROR: Elasticsearch version option is mutually exclusive with any other option")),
                args);
    }

    private void runTestThatVersionIsReturned(String... args) throws Exception {
        runTestVersion(ExitCodes.OK, output -> {
            assertThat(output, containsString("Version: " + Version.CURRENT.toString()));
            assertThat(output, containsString("Build: " + Build.CURRENT.shortHash() + "/" + Build.CURRENT.date()));
            assertThat(output, containsString("JVM: " + JvmInfo.jvmInfo().version()));
        }, args);
    }

    private void runTestVersion(int expectedStatus, Consumer<String> outputConsumer, String... args) throws Exception {
        runTest(expectedStatus, false, outputConsumer, (foreground, pidFile, esSettings) -> {}, args);
    }

    public void testPositionalArgs() throws Exception {
        runTest(
            ExitCodes.USAGE,
            false,
            output -> assertThat(output, containsString("Positional arguments not allowed, found [foo]")),
            (foreground, pidFile, esSettings) -> {},
            "foo"
        );
        runTest(
            ExitCodes.USAGE,
            false,
            output -> assertThat(output, containsString("Positional arguments not allowed, found [foo, bar]")),
            (foreground, pidFile, esSettings) -> {},
            "foo", "bar"
        );
        runTest(
            ExitCodes.USAGE,
            false,
            output -> assertThat(output, containsString("Positional arguments not allowed, found [foo]")),
            (foreground, pidFile, esSettings) -> {},
            "-E", "foo=bar", "foo", "-E", "baz=qux"
        );
    }

    public void testThatPidFileCanBeConfigured() throws Exception {
        runPidFileTest(ExitCodes.USAGE, false, output -> assertThat(output, containsString("Option p/pidfile requires an argument")), "-p");
        runPidFileTest(ExitCodes.OK, true, output -> {}, "-p", "/tmp/pid");
        runPidFileTest(ExitCodes.OK, true, output -> {}, "--pidfile", "/tmp/pid");
    }

    private void runPidFileTest(final int expectedStatus, final boolean expectedInit, Consumer<String> outputConsumer, final String... args)
            throws Exception {
        runTest(
                expectedStatus,
                expectedInit,
                outputConsumer,
                (foreground, pidFile, esSettings) -> assertThat(pidFile, equalTo("/tmp/pid")),
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
                output -> {},
                (foreground, pidFile, esSettings) -> assertThat(foreground, equalTo(!expectedDaemonize)),
                args);
    }

    public void testElasticsearchSettings() throws Exception {
        runTest(
                ExitCodes.OK,
                true,
                output -> {},
                (foreground, pidFile, esSettings) -> {
                    assertThat(esSettings.size(), equalTo(2));
                    assertThat(esSettings, hasEntry("foo", "bar"));
                    assertThat(esSettings, hasEntry("baz", "qux"));
                },
                "-Efoo=bar", "-E", "baz=qux"
        );
    }

    public void testElasticsearchSettingCanNotBeEmpty() throws Exception {
        runTest(
                ExitCodes.USAGE,
                false,
                output -> assertThat(output, containsString("Setting [foo] must not be empty")),
                (foreground, pidFile, esSettings) -> {},
                "-E", "foo="
        );
    }

    public void testUnknownOption() throws Exception {
        runTest(
                ExitCodes.USAGE,
                false,
                output -> assertThat(output, containsString("network.host is not a recognized option")),
                (foreground, pidFile, esSettings) -> {},
                "--network.host");
    }

}
