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

    // Check that for non-Docker, environment variables are not translated into settings
    public void testNonDockerEnvVarSettingsIgnored() throws Exception {
        mockEnvVars.put("ES_SETTING_FOO_BAR", "baz");
        mockEnvVars.put("some.setting", "1");
        callback = env -> {
            Settings settings = env.settings();
            assertThat(settings.hasValue("foo.bar"), is(false));
            assertThat(settings.hasValue("some.settings"), is(false));
        };
        execute();
    }

    // Check that for Docker, environment variables that do not match the criteria for translation to settings are ignored.
    public void testDockerEnvVarSettingsIgnored() throws Exception {
        // No ES_SETTING_ prefix
        mockEnvVars.put("XPACK_SECURITY_FIPS__MODE_ENABLED", "false");
        // Incomplete prefix
        mockEnvVars.put("ES_XPACK_SECURITY_FIPS__MODE_ENABLED", "false");
        // Not underscore-separated
        mockEnvVars.put("ES.SETTING.XPACK.SECURITY.FIPS_MODE.ENABLED", "false");
        // Not uppercase
        mockEnvVars.put("es_setting_xpack_security_fips__mode_enabled", "false");
        // single word is not translated, it must contain a dot
        mockEnvVars.put("singleword", "value");
        // any uppercase letters cause the var to be ignored
        mockEnvVars.put("setting.Ignored", "value");
        callback = env -> {
            Settings settings = env.settings();
            assertThat(settings.hasValue("xpack.security.fips_mode.enabled"), is(false));
            assertThat(settings.hasValue("singleword"), is(false));
            assertThat(settings.hasValue("setting.Ignored"), is(false));
            assertThat(settings.hasValue("setting.ignored"), is(false));
        };
        execute();
    }

    // Check that for Docker builds, various env vars are translated correctly to settings
    public void testDockerEnvVarSettingsTranslated() throws Exception {
        buildType = Build.Type.DOCKER;
        // normal setting with a dot
        mockEnvVars.put("ES_SETTING_SIMPLE_SETTING", "value");
        // double underscore is translated to literal underscore
        mockEnvVars.put("ES_SETTING_UNDERSCORE__HERE", "value");
        // literal underscore and a dot
        mockEnvVars.put("ES_SETTING_UNDERSCORE__DOT_BAZ", "value");
        // two literal underscores
        mockEnvVars.put("ES_SETTING_DOUBLE____UNDERSCORE", "value");
        // literal underscore followed by a dot (not valid setting, but translated nonetheless
        mockEnvVars.put("ES_SETTING_TRIPLE___BAZ", "value");
        // lowercase
        mockEnvVars.put("lowercase.setting", "value");
        callback = env -> {
            Settings settings = env.settings();
            assertThat(settings.get("simple.setting"), equalTo("value"));
            assertThat(settings.get("underscore_here"), equalTo("value"));
            assertThat(settings.get("underscore_dot.baz"), equalTo("value"));
            assertThat(settings.get("triple_.baz"), equalTo("value"));
            assertThat(settings.get("double__underscore"), equalTo("value"));
            assertThat(settings.get("lowercase.setting"), equalTo("value"));
        };
        execute();
    }

    // Check that for Docker builds, env vars takes precedence over settings on the command line.
    public void testDockerEnvVarSettingsOverrideCommandLine() throws Exception {
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
