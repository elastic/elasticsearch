/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.cli;

import joptsimple.OptionSet;

import org.elasticsearch.Build;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EnvironmentAwareCommandTests extends CommandTestCase {

    private Build.Type buildType;
    private Map<String, String> mockEnvVars;
    private Consumer<Environment> callback;

    @Before
    public void resetHooks() {
        buildType = Build.Type.TAR;
        mockEnvVars = new HashMap<>();
        callback = null;
    }

    @Override
    protected Command newCommand() {
        return new EnvironmentAwareCommand("test command") {
            @Override
            protected void execute(Terminal terminal, OptionSet options, Environment env) {
                if (callback != null) {
                    callback.accept(env);
                }
            }

            @Override
            protected Map<String, String> captureSystemProperties() {
                return mockSystemProperties(createTempDir());
            }

            @Override
            protected Map<String, String> captureEnvironmentVariables() {
                return mockEnvVars;
            }

            @Override
            protected Build.Type getBuildType() {
                return buildType;
            }
        };
    }

    public void testNonDockerEnvVarsIgnored() throws Exception {
        mockEnvVars.put("ES_SETTING_FOO_BAR", "baz");
        mockEnvVars.put("some.setting", "1");
        callback = env -> {
            Settings settings = env.settings();
            assertThat(settings.hasValue("foo.bar"), is(false));
            assertThat(settings.hasValue("some.settings"), is(false));
        };
        execute();
    }

    public void testDockerEnvSettings() throws Exception {
        buildType = Build.Type.DOCKER;
        mockEnvVars.put("ES_SETTING_SIMPLE_SETTING", "baz");
        mockEnvVars.put("ES_SETTING_UNDERSCORE__HERE", "baz2");
        mockEnvVars.put("ES_SETTING_UNDERSCORE__DOT_BAZ", "buzz");
        mockEnvVars.put("ES_SETTING_DOUBLE____UNDERSCORE", "buzz");
        mockEnvVars.put("ES_SETTING_TRIPLE___BAZ", "buzz");
        mockEnvVars.put("lowercase.setting", "baz");
        mockEnvVars.put("setting.Ignored", "baz");
        callback = env -> {
            Settings settings = env.settings();
            assertThat(settings.hasValue("simple.setting"), is(true));
            assertThat(settings.hasValue("underscore_here"), is(true));
            assertThat(settings.hasValue("underscore_dot.baz"), is(true));
            assertThat(settings.hasValue("triple_.baz"), is(true));
            assertThat(settings.hasValue("double__underscore"), is(true));
            assertThat(settings.hasValue("lowercase.setting"), is(true));
            assertThat(settings.hasValue("setting.Ignored"), is(false));
        };
        execute();
    }

    public void testDockerEnvSettingsOverride() throws Exception {
        // docker env takes precedence over settings on the command line
        buildType = Build.Type.DOCKER;
        mockEnvVars.put("ES_SETTING_SIMPLE_SETTING", "override");
        callback = env -> {
            Settings settings = env.settings();
            assertThat(settings.get("simple.setting"), equalTo("override"));
        };
        execute("-Esimple.setting=original");
    }
}
