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

package org.elasticsearch.common.logging;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class LoggingConfigurationTests extends ESTestCase {

    @Before
    public void before() throws Exception {
        LogConfigurator.reset();
    }

    public void testResolveMultipleConfigs() throws Exception {
        String level = ESLoggerFactory.getLogger("test").getLevel();
        try {
            Path configDir = getDataPath("config");
            Settings settings = Settings.builder()
                    .put(Environment.PATH_CONF_SETTING.getKey(), configDir.toAbsolutePath())
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build();
            LogConfigurator.configure(settings, true);

            ESLogger esLogger = ESLoggerFactory.getLogger("test");
            Logger logger = esLogger.getLogger();
            Appender appender = logger.getAppender("console");
            assertThat(appender, notNullValue());

            esLogger = ESLoggerFactory.getLogger("second");
            logger = esLogger.getLogger();
            appender = logger.getAppender("console2");
            assertThat(appender, notNullValue());

            esLogger = ESLoggerFactory.getLogger("third");
            logger = esLogger.getLogger();
            appender = logger.getAppender("console3");
            assertThat(appender, notNullValue());
        } finally {
            ESLoggerFactory.getLogger("test").setLevel(level);
        }
    }

    public void testResolveJsonLoggingConfig() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf = tmpDir.resolve(loggingConfiguration("json"));
        Files.write(loggingConf, "{\"json\": \"foo\"}".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put(Environment.PATH_CONF_SETTING.getKey(), tmpDir.toAbsolutePath())
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("json"), is("foo"));
    }

    public void testResolvePropertiesLoggingConfig() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf = tmpDir.resolve(loggingConfiguration("properties"));
        Files.write(loggingConf, "key: value".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put(Environment.PATH_CONF_SETTING.getKey(), tmpDir.toAbsolutePath())
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("key"), is("value"));
    }

    public void testResolveYamlLoggingConfig() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf1 = tmpDir.resolve(loggingConfiguration("yml"));
        Path loggingConf2 = tmpDir.resolve(loggingConfiguration("yaml"));
        Files.write(loggingConf1, "yml: bar".getBytes(StandardCharsets.UTF_8));
        Files.write(loggingConf2, "yaml: bar".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put(Environment.PATH_CONF_SETTING.getKey(), tmpDir.toAbsolutePath())
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), is("bar"));
        assertThat(logSettings.get("yaml"), is("bar"));
    }

    public void testResolveConfigInvalidFilename() throws Exception {
        Path tmpDir = createTempDir();
        Path invalidSuffix = tmpDir.resolve(loggingConfiguration(randomFrom(LogConfigurator.ALLOWED_SUFFIXES)) + randomInvalidSuffix());
        Files.write(invalidSuffix, "yml: bar".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put(Environment.PATH_CONF_SETTING.getKey(), invalidSuffix.toAbsolutePath())
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), nullValue());
    }

    // tests that custom settings are not overwritten by settings in the config file
    public void testResolveOrder() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf = tmpDir.resolve(loggingConfiguration("yaml"));
        Files.write(loggingConf, "logger.test_resolve_order: INFO, file\n".getBytes(StandardCharsets.UTF_8));
        Files.write(loggingConf, "appender.file.type: file\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        Environment environment = InternalSettingsPreparer.prepareEnvironment(
                Settings.builder()
                        .put(Environment.PATH_CONF_SETTING.getKey(), tmpDir.toAbsolutePath())
                        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                        .put("logger.test_resolve_order", "TRACE, console")
                        .put("appender.console.type", "console")
                        .put("appender.console.layout.type", "consolePattern")
                        .put("appender.console.layout.conversionPattern", "[%d{ISO8601}][%-5p][%-25c] %m%n")
                        .build(), new MockTerminal());
        LogConfigurator.configure(environment.settings(), true);
        // args should overwrite whatever is in the config
        ESLogger esLogger = ESLoggerFactory.getLogger("test_resolve_order");
        Logger logger = esLogger.getLogger();
        Appender appender = logger.getAppender("console");
        assertThat(appender, notNullValue());
        assertTrue(logger.isTraceEnabled());
        appender = logger.getAppender("file");
        assertThat(appender, nullValue());
    }

    // tests that config file is not read when we call LogConfigurator.configure(Settings, false)
    public void testConfigNotRead() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf = tmpDir.resolve(loggingConfiguration("yaml"));
        Files.write(loggingConf,
                Arrays.asList(
                        "logger.test_config_not_read: INFO, console",
                        "appender.console.type: console"),
                StandardCharsets.UTF_8);
        Environment environment = InternalSettingsPreparer.prepareEnvironment(
                Settings.builder()
                        .put(Environment.PATH_CONF_SETTING.getKey(), tmpDir.toAbsolutePath())
                        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                        .build(), new MockTerminal());
        LogConfigurator.configure(environment.settings(), false);
        ESLogger esLogger = ESLoggerFactory.getLogger("test_config_not_read");

        assertNotNull(esLogger);
        Logger logger = esLogger.getLogger();
        Appender appender = logger.getAppender("console");
        // config was not read
        assertNull(appender);
    }

    private static String loggingConfiguration(String suffix) {
        return "logging." + randomAsciiOfLength(randomIntBetween(0, 10)) + "." + suffix;
    }

    private static String randomInvalidSuffix() {
        String randomSuffix;
        do {
            randomSuffix = randomAsciiOfLength(randomIntBetween(1, 5));
        } while (LogConfigurator.ALLOWED_SUFFIXES.contains(randomSuffix));
        return randomSuffix;
    }
}
