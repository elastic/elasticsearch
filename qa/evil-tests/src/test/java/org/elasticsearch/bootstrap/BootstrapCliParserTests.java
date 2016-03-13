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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import joptsimple.OptionException;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "modifies system properties intentionally")
public class BootstrapCliParserTests extends CommandTestCase {

    @Override
    protected Command newCommand() {
        return new BootstrapCliParser();
    }

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

    void assertShouldRun(boolean shouldRun) {
        BootstrapCliParser parser = (BootstrapCliParser)command;
        assertEquals(shouldRun, parser.shouldRun());
    }

    public void testVersion() throws Exception {
        String output = execute("-V");
        assertTrue(output, output.contains(Version.CURRENT.toString()));
        assertTrue(output, output.contains(Build.CURRENT.shortHash()));
        assertTrue(output, output.contains(Build.CURRENT.date()));
        assertTrue(output, output.contains(JvmInfo.jvmInfo().version()));
        assertShouldRun(false);

        terminal.reset();
        output = execute("--version");
        assertTrue(output, output.contains(Version.CURRENT.toString()));
        assertTrue(output, output.contains(Build.CURRENT.shortHash()));
        assertTrue(output, output.contains(Build.CURRENT.date()));
        assertTrue(output, output.contains(JvmInfo.jvmInfo().version()));
        assertShouldRun(false);
    }

    public void testPidfile() throws Exception {
        registerProperties("es.pidfile");

        // missing argument
        OptionException e = expectThrows(OptionException.class, () -> {
           execute("-p");
        });
        assertEquals("Option p/pidfile requires an argument", e.getMessage());
        assertShouldRun(false);

        // good cases
        terminal.reset();
        execute("--pidfile", "/tmp/pid");
        assertSystemProperty("es.pidfile", "/tmp/pid");
        assertShouldRun(true);

        System.clearProperty("es.pidfile");
        terminal.reset();
        execute("-p", "/tmp/pid");
        assertSystemProperty("es.pidfile", "/tmp/pid");
        assertShouldRun(true);
    }

    public void testNoDaemonize() throws Exception {
        registerProperties("es.foreground");

        execute();
        assertSystemProperty("es.foreground", null);
        assertShouldRun(true);
    }

    public void testDaemonize() throws Exception {
        registerProperties("es.foreground");

        execute("-d");
        assertSystemProperty("es.foreground", "false");
        assertShouldRun(true);

        System.clearProperty("es.foreground");
        execute("--daemonize");
        assertSystemProperty("es.foreground", "false");
        assertShouldRun(true);
    }

    public void testConfig() throws Exception {
        registerProperties("es.foo", "es.spam");

        execute("-Dfoo=bar", "-Dspam=eggs");
        assertSystemProperty("es.foo", "bar");
        assertSystemProperty("es.spam", "eggs");
        assertShouldRun(true);
    }

    public void testConfigMalformed() throws Exception {
        UserError e = expectThrows(UserError.class, () -> {
            execute("-Dfoo");
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Malformed elasticsearch setting"));
    }

    public void testUnknownOption() throws Exception {
        OptionException e = expectThrows(OptionException.class, () -> {
            execute("--network.host");
        });
        assertTrue(e.getMessage(), e.getMessage().contains("network.host is not a recognized option"));
    }

    private void registerProperties(String ... systemProperties) {
        propertiesToClear.addAll(Arrays.asList(systemProperties));
    }

    private void assertSystemProperty(String name, String expectedValue) throws Exception {
        String msg = String.format(Locale.ROOT, "Expected property %s to be %s, terminal output was %s", name, expectedValue, terminal.getOutput());
        assertThat(msg, System.getProperty(name), is(expectedValue));
    }
}
