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
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
