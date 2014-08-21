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

package org.elasticsearch.common.logging.log4j;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class LoggingConfigurationTests extends ElasticsearchTestCase {

    @Before
    public void before() throws Exception {
        LogConfigurator.reset();
    }

    @Test
    public void testResolveMultipleConfigs() throws Exception {
        String level = Log4jESLoggerFactory.getLogger("test").getLevel();
        try {
            Path configDir = getDataPath("config");
            Settings settings = Settings.builder()
                    .put("path.conf", configDir.toAbsolutePath())
                    .put("path.home", createTempDir().toString())
                    .build();
            LogConfigurator.configure(settings);

            ESLogger esLogger = Log4jESLoggerFactory.getLogger("test");
            Logger logger = ((Log4jESLogger) esLogger).logger();
            Appender appender = logger.getAppender("console");
            assertThat(appender, notNullValue());

            esLogger = Log4jESLoggerFactory.getLogger("second");
            logger = ((Log4jESLogger) esLogger).logger();
            appender = logger.getAppender("console2");
            assertThat(appender, notNullValue());

            esLogger = Log4jESLoggerFactory.getLogger("third");
            logger = ((Log4jESLogger) esLogger).logger();
            appender = logger.getAppender("console3");
            assertThat(appender, notNullValue());
        } finally {
            Log4jESLoggerFactory.getLogger("test").setLevel(level);
        }
    }

    @Test
    public void testResolveJsonLoggingConfig() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf = tmpDir.resolve(loggingConfiguration("json"));
        Files.write(loggingConf, "{\"json\": \"foo\"}".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put("path.conf", tmpDir.toAbsolutePath())
                    .put("path.home", createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("json"), is("foo"));
    }

    @Test
    public void testResolvePropertiesLoggingConfig() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf = tmpDir.resolve(loggingConfiguration("properties"));
        Files.write(loggingConf, "key: value".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put("path.conf", tmpDir.toAbsolutePath())
                    .put("path.home", createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("key"), is("value"));
    }

    @Test
    public void testResolveYamlLoggingConfig() throws Exception {
        Path tmpDir = createTempDir();
        Path loggingConf1 = tmpDir.resolve(loggingConfiguration("yml"));
        Path loggingConf2 = tmpDir.resolve(loggingConfiguration("yaml"));
        Files.write(loggingConf1, "yml: bar".getBytes(StandardCharsets.UTF_8));
        Files.write(loggingConf2, "yaml: bar".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put("path.conf", tmpDir.toAbsolutePath())
                    .put("path.home", createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), is("bar"));
        assertThat(logSettings.get("yaml"), is("bar"));
    }

    @Test
    public void testResolveConfigInvalidFilename() throws Exception {
        Path tmpDir = createTempDir();
        Path invalidSuffix = tmpDir.resolve(loggingConfiguration(randomFrom(LogConfigurator.ALLOWED_SUFFIXES)) + randomInvalidSuffix());
        Files.write(invalidSuffix, "yml: bar".getBytes(StandardCharsets.UTF_8));
        Environment environment = new Environment(
                Settings.builder()
                    .put("path.conf", invalidSuffix.toAbsolutePath())
                    .put("path.home", createTempDir().toString())
                    .build());

        Settings.Builder builder = Settings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), nullValue());
    }

    @Test
    public void testResolveLoggingDirFromSystemProperty() throws Exception {
        Path tempDir = createTempDir();
        Path customLogConfig1 = tempDir.resolve(LoggingConfigurationTests.loggingConfiguration("yml"));
        Files.write(customLogConfig1, "action: WARN".getBytes(StandardCharsets.UTF_8));
        Path customLogConfig2 = tempDir.resolve(LoggingConfigurationTests.loggingConfiguration("yml"));
        Files.write(customLogConfig2, "transport: TRACE".getBytes(StandardCharsets.UTF_8));
        if (randomBoolean()) {
            //gets ignored because the filename doesn't start with the "logging." prefix
            Path ignoredLogConfig = tempDir.resolve("invalid_logging.yml");
            Files.write(ignoredLogConfig, "invalid: TRACE".getBytes(StandardCharsets.UTF_8));
        }
        if (randomBoolean()) {
            //gets ignored because the filename doesn't end with one of the allowed suffixes
            Path ignoredLogConfig = tempDir.resolve("logging.unknown");
            Files.write(ignoredLogConfig, "invalid: TRACE".getBytes(StandardCharsets.UTF_8));
        }

        try {
            System.setProperty(LogConfigurator.ES_LOGGING_SYSPROP, tempDir.toString());
            Environment environment = new Environment(Settings.EMPTY);
            Settings.Builder builder = Settings.builder();
            LogConfigurator.resolveConfig(environment, builder);

            Settings logSettings = builder.build();
            assertThat(logSettings.names().size(), equalTo(2));
            assertThat(logSettings.get("action"), is("WARN"));
            assertThat(logSettings.get("transport"), is("TRACE"));
            assertThat(logSettings.get("invalid"), nullValue());
        } finally {
            System.clearProperty(LogConfigurator.ES_LOGGING_SYSPROP);
        }
    }

    @Test
    public void testResolveLoggingFileFromSystemProperty() throws Exception {
        Path tempDir = createTempDir();
        Path customLogConfig = tempDir.resolve(LoggingConfigurationTests.loggingConfiguration("yml"));
        Files.write(customLogConfig, "action: WARN".getBytes(StandardCharsets.UTF_8));
        if (randomBoolean()) {
            //gets ignored because the sysprop points directly to the above file
            Path ignoredLogConfig = tempDir.resolve(LoggingConfigurationTests.loggingConfiguration("yml"));
            Files.write(ignoredLogConfig, "transport: TRACE".getBytes(StandardCharsets.UTF_8));
        }

        try {
            System.setProperty(LogConfigurator.ES_LOGGING_SYSPROP, customLogConfig.toString());
            Environment environment = new Environment(Settings.EMPTY);
            Settings.Builder builder = Settings.builder();
            LogConfigurator.resolveConfig(environment, builder);

            Settings logSettings = builder.build();
            assertThat(logSettings.names().size(), equalTo(1));
            assertThat(logSettings.get("action"), is("WARN"));
        } finally {
            System.clearProperty(LogConfigurator.ES_LOGGING_SYSPROP);
        }
    }

    @Test(expected = FailedToResolveConfigException.class)
    public void testResolveNonExistingLoggingFileFromSystemProperty() throws IOException {
        try {
            System.setProperty(LogConfigurator.ES_LOGGING_SYSPROP, "non_existing.yml");
            Environment environment = new Environment(Settings.builder().build());
            Settings.Builder builder = Settings.builder();
            LogConfigurator.resolveConfig(environment, builder);
        } finally {
            System.clearProperty(LogConfigurator.ES_LOGGING_SYSPROP);
        }
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
