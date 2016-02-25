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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.StreamsUtils;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@SuppressForbidden(reason = "modifies system properties intentionally")
public class ElasticsearchCommandLineParsingTests extends CliToolTestCase {

    private CaptureOutputTerminal terminal = new CaptureOutputTerminal();
    private List<String> propertiesToClear = new ArrayList<>();
    private Map<Object, Object> properties;

    @Before
    public void before() {
        this.properties = new HashMap<>(System.getProperties());
    }

    @After
    public void clearProperties() {
        for (String property : propertiesToClear) {
            System.clearProperty(property);
        }
        propertiesToClear.clear();
        assertEquals("properties leaked", properties, new HashMap<>(System.getProperties()));
    }

    public void testThatVersionIsReturned() throws Exception {
        int status = Elasticsearch.main(new String[] { "version" }, terminal);
        assertStatus(status, 0);

        assertThatTerminalOutput(containsString(Version.CURRENT.toString()));
        assertThatTerminalOutput(containsString(Build.CURRENT.shortHash()));
        assertThatTerminalOutput(containsString(Build.CURRENT.date()));
        assertThatTerminalOutput(containsString(JvmInfo.jvmInfo().version()));
    }

    public void testThatPidFileCanBeConfigured() throws Exception {
        Elasticsearch.Command command = Elasticsearch.parse(args("start --pidfile")); // missing pid file
        assertThat(command, instanceOf(Elasticsearch.HelpCommand.class));
        assertStatus(command.status, 1);

        // good cases
        testPidFile("--pidfile");
        testPidFile("-p");
    }

    private void testPidFile(String parameter) {
        Elasticsearch.Command command = Elasticsearch.parse(args("start " + parameter + " /tmp/pid"));
        assertStatus(command.status, 0);
        assertThat(command, instanceOf(Elasticsearch.StartCommand.class));
        Elasticsearch.StartCommand startCommand = (Elasticsearch.StartCommand)command;
        assertThat(startCommand.pidFile, equalTo("/tmp/pid"));
    }

    public void testThatParsingDaemonizeWorks() throws Exception {
        testDaemonize("-d");
        testDaemonize("--daemonize");
    }

    private void testDaemonize(String parameter) {
        Elasticsearch.Command command = Elasticsearch.parse(args("start " + parameter));
        assertStatus(command.status, 0);
        assertThat(command, instanceOf(Elasticsearch.StartCommand.class));
        Elasticsearch.StartCommand startCommand = (Elasticsearch.StartCommand)command;
        assertTrue(startCommand.daemonize);
    }


    public void testThatJavaPropertyStyleArgumentsCanBeParsed() throws Exception {
        registerProperties("es.foo", "es.spam");
        Elasticsearch.Command command = Elasticsearch.parse(args("start -E es.foo=bar -E es.spam=eggs"));
        assertStatus(command.status, 0);
        assertThat(command, instanceOf(Elasticsearch.StartCommand.class));
        Elasticsearch.StartCommand startCommand = (Elasticsearch.StartCommand)command;
        assertSetting(startCommand.settings, "es.foo", "bar");
        assertSetting(startCommand.settings, "es.spam", "eggs");
    }

    private void assertSetting(Map<String, String> settings, String key, String value) {
        assertTrue(settings.containsKey(key));
        assertThat(settings.get(key), equalTo(value));
        assertSystemProperty(key, value);
    }

    public void testThatJavaPropertyMustStartWithPrefix() {
        try {
            Elasticsearch.parse(args("start -E foo=bar"));
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Elasticsearch settings must be prefixed with \"es.\" but was \"foo\""));
        }
    }

    public void testThatUnknownLongOptionsCanBeParsed() throws Exception {
        registerProperties("es.network.host", "es.my.option");

        Elasticsearch.Command command = Elasticsearch.parse(args("start -E es.network.host=127.0.0.1 -E es.my.option=true"));
        assertStatus(command.status, 0);
        assertThat(command, instanceOf(Elasticsearch.StartCommand.class));
        Elasticsearch.StartCommand startCommand = (Elasticsearch.StartCommand)command;
        assertSetting(startCommand.settings, "es.network.host", "127.0.0.1");
        assertSetting(startCommand.settings, "es.my.option", "true");
    }

    public void testThatUnknownLongOptionsNeedAValue() throws Exception {
        registerProperties("es.network.host");

        Elasticsearch.Command command = Elasticsearch.parse(args("start -E network.host"));
        assertStatus(command.status, 1);
        assertThat(command, instanceOf(Elasticsearch.HelpCommand.class));
        Elasticsearch.HelpCommand helpCommand = (Elasticsearch.HelpCommand)command;
        assertThat(helpCommand.message, containsString("for parameter [network.host]"));
    }

    public void testParsingErrors() throws Exception {
        // unknown params
        Elasticsearch.Command command = Elasticsearch.parse(args("version --unknown-param /tmp/pid"));
        assertStatus(command.status, 1);
        assertThat(command, instanceOf(Elasticsearch.HelpCommand.class));
        Elasticsearch.HelpCommand helpCommand = (Elasticsearch.HelpCommand)command;
        assertThat(helpCommand.message, containsString("\"--unknown-param\" is not a valid option"));

        // free floating value
        command = Elasticsearch.parse(args("start 127.0.0.1"));
        assertStatus(command.status, 1);
        helpCommand = (Elasticsearch.HelpCommand)command;
        assertThat(helpCommand.message, containsString("No argument is allowed: 127.0.0.1"));
    }

    public void testHelpWorks() throws Exception {
        Map<String, Tuple<String, Integer>> entries = new HashMap<>();
        List<Tuple<String, String>> tuples = new ArrayList<>();
        entries.put("version --help", new Tuple<>("elasticsearch-version.help", 0));
        entries.put("version -h", new Tuple<>("elasticsearch-version.help", 0));
        entries.put("start --help", new Tuple<>("elasticsearch-start.help", 0));
        entries.put("start -h", new Tuple<>("elasticsearch-start.help", 0));
        entries.put("--help", new Tuple<>("elasticsearch.help", 1));
        entries.put("-h", new Tuple<>("elasticsearch-h.help", 1));

        for (Map.Entry<String, Tuple<String, Integer>> entry : entries.entrySet()) {
            Elasticsearch.Command command = Elasticsearch.parse(args(entry.getKey()));
            assertStatus(command.status, entry.getValue().v2());
            assertThat(command, instanceOf(Elasticsearch.HelpCommand.class));
            Elasticsearch.HelpCommand helpCommand = (Elasticsearch.HelpCommand)command;
            assertHelpMessageContainsHelpFile(helpCommand.message, "/org/elasticsearch/bootstrap/" + entry.getValue().v1());
        }
    }

    public void testThatSpacesInParametersAreSupported() throws Exception {
        // emulates: bin/elasticsearch -E node.name "'my node with spaces'" --pidfile "'/tmp/my pid.pid'"
        registerProperties("es.my.param");

        Elasticsearch.Command command =
            Elasticsearch.parse(new String[] { "start", "--pidfile", "foo with space", "-E", "es.my.param=my awesome neighbour" });
        assertThat(command, instanceOf(Elasticsearch.StartCommand.class));
        Elasticsearch.StartCommand startCommand = (Elasticsearch.StartCommand)command;
        assertThat(startCommand.pidFile, equalTo("foo with space"));
        assertSetting(startCommand.settings, "es.my.param", "my awesome neighbour");
    }

    private void registerProperties(String ... systemProperties) {
        propertiesToClear.addAll(Arrays.asList(systemProperties));
    }

    private void assertSystemProperty(String name, String expectedValue) {
        String msg =
            String.format(
                Locale.ROOT,
                "Expected property %s to be %s, terminal output was %s",
                name,
                expectedValue,
                terminal.getTerminalOutput());
        assertThat(msg, System.getProperty(name), is(expectedValue));
    }

    private void assertStatus(int status, int expectedStatus) {
        assertThat(
            String.format(
                Locale.ROOT,
                "Expected status to be [%s], but was [%s], terminal output was %s",
                expectedStatus,
                status,
                terminal.getTerminalOutput()),
            status,
            is(expectedStatus));
    }

    private void assertThatTerminalOutput(Matcher<String> matcher) {
        assertThat(terminal.getTerminalOutput(), hasItem(matcher));
    }

    private static void assertHelpMessageContainsHelpFile(String message, String classPath) throws IOException {
        List<String> nonEmptyLines = new ArrayList<>();
        String[] lines = message.split("\n");
        for (String line : lines) {
            String originalPrintedLine = line.replaceAll(System.lineSeparator(), "");
            if (!Strings.isNullOrEmpty(originalPrintedLine)) {
                nonEmptyLines.add(originalPrintedLine);
            }
        }
        assertThat(nonEmptyLines, hasSize(greaterThan(0)));

        String expectedDocs = StreamsUtils.copyToStringFromClasspath(classPath);
        for (String nonEmptyLine : nonEmptyLines) {
            assertThat(expectedDocs, containsString(nonEmptyLine.replaceAll(System.lineSeparator(), "")));
        }
    }
}
