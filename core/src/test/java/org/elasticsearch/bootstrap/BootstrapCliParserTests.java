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
import org.elasticsearch.common.cli.CliTool.ExitStatus;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.hamcrest.Matcher;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.cli.CliTool.ExitStatus.*;
import static org.hamcrest.Matchers.*;

public class BootstrapCliParserTests extends CliToolTestCase {

    private CaptureOutputTerminal terminal = new CaptureOutputTerminal();
    private List<String> propertiesToClear = new ArrayList<>();

    @After
    public void clearProperties() {
        for (String property : propertiesToClear) {
            System.clearProperty(property);
        }
    }

    public void testThatVersionIsReturned() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        ExitStatus status = parser.execute(args("version"));
        assertStatus(status, OK_AND_EXIT);

        assertThatTerminalOutput(containsString(Version.CURRENT.toString()));
        assertThatTerminalOutput(containsString(Build.CURRENT.hashShort()));
        assertThatTerminalOutput(containsString(Build.CURRENT.timestamp()));
        assertThatTerminalOutput(containsString(JvmInfo.jvmInfo().version()));
    }

    public void testThatVersionIsReturnedAsStartParameter() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        ExitStatus status = parser.execute(args("start -V"));
        assertStatus(status, OK_AND_EXIT);

        assertThatTerminalOutput(containsString(Version.CURRENT.toString()));
        assertThatTerminalOutput(containsString(Build.CURRENT.hashShort()));
        assertThatTerminalOutput(containsString(Build.CURRENT.timestamp()));
        assertThatTerminalOutput(containsString(JvmInfo.jvmInfo().version()));

        CaptureOutputTerminal terminal = new CaptureOutputTerminal();
        parser = new BootstrapCLIParser(terminal);
        status = parser.execute(args("start --version"));
        assertStatus(status, OK_AND_EXIT);

        assertThatTerminalOutput(containsString(Version.CURRENT.toString()));
        assertThatTerminalOutput(containsString(Build.CURRENT.hashShort()));
        assertThatTerminalOutput(containsString(Build.CURRENT.timestamp()));
        assertThatTerminalOutput(containsString(JvmInfo.jvmInfo().version()));
    }

    public void testThatPidFileCanBeConfigured() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.pidfile");

        ExitStatus status = parser.execute(args("start --pidfile")); // missing pid file
        assertStatus(status, USAGE);

        // good cases
        status = parser.execute(args("start --pidfile /tmp/pid"));
        assertStatus(status, OK);
        assertSystemProperty("es.pidfile", "/tmp/pid");

        System.clearProperty("es.pidfile");
        status = parser.execute(args("start -p /tmp/pid"));
        assertStatus(status, OK);
        assertSystemProperty("es.pidfile", "/tmp/pid");
    }

    public void testThatParsingDaemonizeWorks() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.foreground");

        ExitStatus status = parser.execute(args("start -d"));
        assertStatus(status, OK);
        assertThat(System.getProperty("es.foreground"), is("false"));
    }

    public void testThatNotDaemonizingDoesNotConfigureProperties() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.foreground");

        ExitStatus status = parser.execute(args("start"));
        assertStatus(status, OK);
        assertThat(System.getProperty("es.foreground"), is(nullValue()));
    }

    public void testThatJavaPropertyStyleArgumentsCanBeParsed() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.foo", "es.spam");

        ExitStatus status = parser.execute(args("start -Dfoo=bar -Dspam=eggs"));
        assertStatus(status, OK);
        assertSystemProperty("es.foo", "bar");
        assertSystemProperty("es.spam", "eggs");
    }

    public void testThatJavaPropertyStyleArgumentsWithEsPrefixAreNotPrefixedTwice() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.spam", "es.pidfile");

        ExitStatus status = parser.execute(args("start -Des.pidfile=/path/to/foo/elasticsearch/distribution/zip/target/integ-tests/es.pid -Dspam=eggs"));
        assertStatus(status, OK);
        assertThat(System.getProperty("es.es.pidfile"), is(nullValue()));
        assertSystemProperty("es.pidfile", "/path/to/foo/elasticsearch/distribution/zip/target/integ-tests/es.pid");
        assertSystemProperty("es.spam", "eggs");
    }

    public void testThatUnknownLongOptionsCanBeParsed() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.network.host", "es.my.option");

        ExitStatus status = parser.execute(args("start --network.host 127.0.0.1 --my.option=true"));
        assertStatus(status, OK);
        assertSystemProperty("es.network.host", "127.0.0.1");
        assertSystemProperty("es.my.option", "true");
    }

    public void testThatUnknownLongOptionsNeedAValue() throws Exception {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.network.host");

        ExitStatus status = parser.execute(args("start --network.host"));
        assertStatus(status, USAGE);
        assertThatTerminalOutput(containsString("Parameter [network.host] needs value"));

        status = parser.execute(args("start --network.host --foo"));
        assertStatus(status, USAGE);
        assertThatTerminalOutput(containsString("Parameter [network.host] needs value"));
    }

    public void testParsingErrors() {
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);

        // unknown params
        ExitStatus status = parser.execute(args("version --unknown-param /tmp/pid"));
        assertStatus(status, USAGE);
        assertThatTerminalOutput(containsString("Unrecognized option: --unknown-param"));

        // single dash in extra params
        terminal = new CaptureOutputTerminal();
        parser = new BootstrapCLIParser(terminal);
        status = parser.execute(args("start -network.host 127.0.0.1"));
        assertStatus(status, USAGE);
        assertThatTerminalOutput(containsString("Parameter [-network.host]does not start with --"));

        // never ended parameter
        terminal = new CaptureOutputTerminal();
        parser = new BootstrapCLIParser(terminal);
        status = parser.execute(args("start --network.host"));
        assertStatus(status, USAGE);
        assertThatTerminalOutput(containsString("Parameter [network.host] needs value"));

        // free floating value
        terminal = new CaptureOutputTerminal();
        parser = new BootstrapCLIParser(terminal);
        status = parser.execute(args("start 127.0.0.1"));
        assertStatus(status, USAGE);
        assertThatTerminalOutput(containsString("Parameter [127.0.0.1]does not start with --"));
    }

    public void testHelpWorks() throws Exception {
        List<Tuple<String, String>> tuples = new ArrayList<>();
        tuples.add(new Tuple<>("version --help", "elasticsearch-version.help"));
        tuples.add(new Tuple<>("version -h", "elasticsearch-version.help"));
        tuples.add(new Tuple<>("start --help", "elasticsearch-start.help"));
        tuples.add(new Tuple<>("start -h", "elasticsearch-start.help"));
        tuples.add(new Tuple<>("--help", "elasticsearch.help"));
        tuples.add(new Tuple<>("-h", "elasticsearch.help"));

        for (Tuple<String, String> tuple : tuples) {
            terminal = new CaptureOutputTerminal();
            BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
            ExitStatus status = parser.execute(args(tuple.v1()));
            assertStatus(status, OK_AND_EXIT);
            assertTerminalOutputContainsHelpFile(terminal, "/org/elasticsearch/bootstrap/" + tuple.v2());
        }
    }

    public void testThatSpacesInParametersAreSupported() throws Exception {
        // emulates: bin/elasticsearch --node.name "'my node with spaces'" --pidfile "'/tmp/my pid.pid'"
        BootstrapCLIParser parser = new BootstrapCLIParser(terminal);
        registerProperties("es.pidfile", "es.my.param");

        ExitStatus status = parser.execute("start", "--pidfile", "foo with space", "--my.param", "my awesome neighbour");
        assertStatus(status, OK);
        assertSystemProperty("es.pidfile", "foo with space");
        assertSystemProperty("es.my.param", "my awesome neighbour");

    }

    private void registerProperties(String ... systemProperties) {
        propertiesToClear.addAll(Arrays.asList(systemProperties));
    }

    private void assertSystemProperty(String name, String expectedValue) {
        String msg = String.format(Locale.ROOT, "Expected property %s to be %s, terminal output was %s", name, expectedValue, terminal.getTerminalOutput());
        assertThat(msg, System.getProperty(name), is(expectedValue));
    }

    private void assertStatus(ExitStatus status, ExitStatus expectedStatus) {
        assertThat(String.format(Locale.ROOT, "Expected status to be [%s], but was [%s], terminal output was %s", expectedStatus, status, terminal.getTerminalOutput()), status, is(expectedStatus));
    }

    private void assertThatTerminalOutput(Matcher<String> matcher) {
        assertThat(terminal.getTerminalOutput(), hasItem(matcher));
    }
}
