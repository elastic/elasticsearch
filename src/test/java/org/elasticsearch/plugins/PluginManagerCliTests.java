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

package org.elasticsearch.plugins;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.collect.MapBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class PluginManagerCliTests extends CliToolTestCase {

    private static final Terminal terminal = new MockTerminal();
    private static final String PLUGIN = "plugin_name";

    private static final Map<String, String> INSTALL_OPTIONS_SHORT = MapBuilder.<String, String>newMapBuilder()
            .put("-u", "url")
            .put("-t", "30s").immutableMap();
    private static final Map<String, String> INSTALL_OPTIONS_LONG = MapBuilder.<String, String>newMapBuilder()
            .put("--url", "url")
            .put("--timeout", "30s").immutableMap();

    private static final Map<String, String> UNINSTALL_OPTIONS = MapBuilder.<String, String>newMapBuilder()
            .immutableMap();

    private static final Map<String, String> LIST_OPTIONS = MapBuilder.<String, String>newMapBuilder()
            .immutableMap();

    @Test
    public void testInstall_Options() throws Exception {
        logger.info("  --> install specific options");
        // bin/plugin install plugin_name
        installCommandTester(null, 0l, Collections.EMPTY_MAP);

        // bin/plugin install -u url -t 30s plugin_name
        installCommandTester("url", 30l, INSTALL_OPTIONS_SHORT);
        // bin/plugin install --url url --timeout 30s plugin_name
        installCommandTester("url", 30l, INSTALL_OPTIONS_LONG);

        // We test with no plugin_name which is mandatory
        logger.info("  --> install with no mandatory parameter");
        CliTool.Command.Exit command = (CliTool.Command.Exit) pluginCommandTester("install", null, CliTool.Command.Exit.class);
        assertThat(command.status(), is(CliTool.ExitStatus.USAGE));

        // We test with empty url
        logger.info("  --> install with empty url");
        try {
            command = (CliTool.Command.Exit) pluginCommandTester("install", PLUGIN + " --url  ", CliTool.Command.Exit.class);
            // TODO I think we should catch any MissingArgumentException and return CliTool.ExitStatus.USAGE
            assertThat(command.status(), is(CliTool.ExitStatus.USAGE));
        } catch (MissingArgumentException e) {
            // TODO for now this exception is thrown to the plugin manager tool
        }

        // We test with empty timeout
        logger.info("  --> install with empty timeout");
        try {
            command = (CliTool.Command.Exit) pluginCommandTester("install", PLUGIN + " --timeout  ", CliTool.Command.Exit.class);
            // TODO I think we should catch any MissingArgumentException and return CliTool.ExitStatus.USAGE
            assertThat(command.status(), is(CliTool.ExitStatus.USAGE));
        } catch (MissingArgumentException e) {
            // TODO for now this exception is thrown to the plugin manager tool
        }

        logger.info("  --> install common options");
        commonCommandTester("install", PLUGIN, PluginManager.Install.class);
    }

    @Test
    public void testUninstall_Options() throws Exception {
        logger.info("  --> uninstall specific options");
        uninstallCommandTester();

        // We test with no plugin_name which is mandatory
        logger.info("  --> uninstall with no mandatory parameter");
        CliTool.Command.Exit command = (CliTool.Command.Exit) pluginCommandTester("uninstall", null, CliTool.Command.Exit.class);
        assertThat(command.status(), is(CliTool.ExitStatus.USAGE));

        logger.info("  --> uninstall common options");
        commonCommandTester("uninstall", PLUGIN, PluginManager.Uninstall.class);
    }

    @Test
    public void testList_Options() throws Exception {
        logger.info("  --> list specific options");
        listCommandTester();
        logger.info("  --> list common options");
        commonCommandTester("list", null, PluginManager.ListPlugins.class);
    }

    private CliTool.Command pluginCommandTester(String command, String args, Class<? extends CliTool.Command> clazz) throws Exception {
        logger.info("    --> testing bin/plugin {} {}", command, args == null ? "" : args);
        CliTool pluginTool = new PluginManager(terminal);

        CliTool.Command cmd = pluginTool.parse(command, args(args));
        assertThat(cmd, notNullValue());
        assertThat(cmd, instanceOf(clazz));
        return cmd;
    }

    private void commonCommandTester(String command, String mandatory, Class<? extends CliTool.Command> clazz) throws Exception {
        // We test "-h"
        pluginCommandTester(command, "-h", CliTool.Command.Help.class);
        // We test "--help"
        pluginCommandTester(command, "--help", CliTool.Command.Help.class);

        // Try -s -v at the same time
        try {
            pluginCommandTester(command, optionBuilder(mandatory, "-v -s"), null);
            fail("-v and -s should not be accepted at the same time");
        } catch (AlreadySelectedException e) {
            // We expect that
        }

        // Try unknown option
        try {
            pluginCommandTester(command, optionBuilder(mandatory, "--doesnotexistoption"), null);
            fail("unknown option should not be accepted");
        } catch (UnrecognizedOptionException e) {
            // We expect that
        }
    }

    private String optionBuilder(String mandatory, String args) {
        if (mandatory == null) {
            return args;
        }

        return mandatory + " " + args;
    }

    private String shuffleOptions(String mandatory, Map<String, String> options) {
        // We want to test shuffling options as any order should work
        StringBuffer args = new StringBuffer();
        List<String> keys = new ArrayList(options.keySet());
        boolean mandatorySet = mandatory == null;
        boolean silentOrVerbose = false;

        Collections.shuffle(keys);
        for (String key : keys) {
            if (!mandatorySet && rarely()) {
                append(args, mandatory);
                mandatorySet = true;
            }

            if (!silentOrVerbose) {
                silentOrVerbose = generateSilentOrVerbose(args);
            }

            append(args, key);
            String value = options.get(key);
            if (Strings.hasText(value)) {
                args.append(" ");
                args.append(value);
            }
        }

        if (!silentOrVerbose) {
            generateSilentOrVerbose(args);
        }

        if (!mandatorySet) {
            append(args, mandatory);
        }

        return args.toString();
    }

    private boolean generateSilentOrVerbose(StringBuffer args) {
        if (rarely()) {
            append(args, rarely() ? "-s" : "--silent");
            return true;
        }
        if (rarely()) {
            append(args, rarely() ? "-v" : "--verbose");
            return true;
        }
        return false;
    }

    private void append(StringBuffer sb, String text) {
        if (sb.length() > 0) {
            sb.append(" ");
        }
        sb.append(text);
    }

    private void installCommandTester(String url, long timeout, Map<String, String> options) throws Exception {
        PluginManager.Install install = genericCommandTester("install", PluginManager.Install.class, shuffleOptions(PLUGIN, options));
        assertThat(install.name, notNullValue());
        assertThat(install.name, equalTo(PLUGIN));
        assertThat(install.timeout, notNullValue());
        assertThat(install.timeout.seconds(), equalTo(timeout));

        if (url != null) {
            assertThat(install.url, notNullValue());
            assertThat(install.url, equalTo(url));
        } else {
            assertThat(install.url, nullValue());
        }
    }

    private void uninstallCommandTester() throws Exception {
        PluginManager.Uninstall uninstall = genericCommandTester("uninstall", PluginManager.Uninstall.class, shuffleOptions(PLUGIN, UNINSTALL_OPTIONS));
        assertThat(uninstall.name, notNullValue());
        assertThat(uninstall.name, equalTo(PLUGIN));
    }

    private void listCommandTester() throws Exception {
        genericCommandTester("list", PluginManager.ListPlugins.class, shuffleOptions(null, LIST_OPTIONS));
    }

    private <T extends CliTool.Command> T genericCommandTester(String commandName, Class<T> clazz, String... args) throws Exception {
        CliTool.Command command = null;
        if (args != null) {
            for (String arg : args) {
                command = pluginCommandTester(commandName, arg, clazz);
            }
        } else {
            command = pluginCommandTester(commandName, null, clazz);
        }
        return (T) command;
    }
}
