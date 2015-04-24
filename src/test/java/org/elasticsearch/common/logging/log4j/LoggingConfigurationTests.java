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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
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
            Settings settings = ImmutableSettings.builder()
                    .put("path.conf", configDir.toAbsolutePath())
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
                ImmutableSettings.builder().put("path.conf", tmpDir.toAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
                ImmutableSettings.builder().put("path.conf", tmpDir.toAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
                ImmutableSettings.builder().put("path.conf", tmpDir.toAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
                ImmutableSettings.builder().put("path.conf", invalidSuffix.toAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), Matchers.nullValue());
    }

    @Test
    public void testIncorrectLogFileNameIsNotLoaded() throws IOException {

        try {
            File customLoggingDir = new RandomizedTest().newTempDir();

            File customLogConfig = new File(customLoggingDir, "invalid_logging.yml");
            com.google.common.io.Files.write("foo: TRACE", customLogConfig, StandardCharsets.UTF_8);

            // the file directory
            System.setProperty(LogConfigurator.ES_LOGGING_PROP_NAME, customLogConfig.getAbsolutePath());

            Environment environment = new Environment(ImmutableSettings.builder().build());
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            LogConfigurator.resolveConfig(environment, builder);

            Settings logSettings = builder.build();

            assertThat(logSettings.get("foo"), nullValue());
        } finally {
            System.clearProperty(LogConfigurator.ES_LOGGING_PROP_NAME);
        }
    }

    /**
     * One log file is in classpath and the other one is set using the system property.
     * The log file which is set by the system property is not loaded which throws an exception.
     *
     * @throws IOException
     */
    @Test(expected = FailedToResolveConfigException.class)
    public void testIncorrectLogFileNameIsNotLoadedFromClasspath_1() throws IOException {

        try {
            File customLoggingDir = new RandomizedTest().newTempDir();

            File loggingFile = new File(customLoggingDir, "invalid_logging.yml");
            com.google.common.io.Files.write("foo: bar", loggingFile, StandardCharsets.UTF_8);

            String strClassPath = System.getProperty("java.class.path");
            System.setProperty("java.class.path", strClassPath + ":" + customLoggingDir);

            //the file name
            System.setProperty(LogConfigurator.ES_LOGGING_PROP_NAME, "custom_log.yml");

            Environment environment = new Environment(ImmutableSettings.builder().build());
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            LogConfigurator.resolveConfig(environment, builder);
        } finally {
            System.clearProperty(LogConfigurator.ES_LOGGING_PROP_NAME);
        }
    }

    /**
     * @throws Exception
     * @see LoggingConfigurationTests#testResolveYamlLoggingConfig() - similar but this test case
     * uses the system property es.logging
     */
    @Test
    public void testResolveYamlLoggingConfigUsingSystemProperty() throws Exception {
        try {
            File tmpDir = new RandomizedTest().newTempDir();
            File loggingConf1 = new File(tmpDir, LoggingConfigurationTests.loggingConfiguration("yml"));
            File loggingConf2 = new File(tmpDir, LoggingConfigurationTests.loggingConfiguration("yml"));
            com.google.common.io.Files.write("yml: bar", loggingConf1, StandardCharsets.UTF_8);
            com.google.common.io.Files.write("yaml: bar", loggingConf2, StandardCharsets.UTF_8);

            System.setProperty(LogConfigurator.ES_LOGGING_PROP_NAME, tmpDir.getAbsolutePath());

            Environment environment = new Environment(ImmutableSettings.builder().build());

            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            LogConfigurator.resolveConfig(environment, builder);

            Settings logSettings = builder.build();
            assertThat(logSettings.get("yml"), is("bar"));
            assertThat(logSettings.get("yaml"), is("bar"));
        } finally {
            System.clearProperty(LogConfigurator.ES_LOGGING_PROP_NAME);
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
