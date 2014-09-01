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

import com.google.common.io.Files;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.elasticsearch.common.logging.log4j.Log4jESLogger;
import org.elasticsearch.common.logging.log4j.Log4jESLoggerFactory;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;

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
    public void testMultipleConfigs() throws Exception {
        File configDir = resolveConfigDir();
        Settings settings = ImmutableSettings.builder()
                .put("path.conf", configDir.getAbsolutePath())
                .build();
        LogConfigurator.configure(settings);

        ESLogger esLogger = Log4jESLoggerFactory.getLogger("first");
        Logger logger = ((Log4jESLogger) esLogger).logger();
        Appender appender = logger.getAppender("console1");
        assertThat(appender, notNullValue());

        esLogger = Log4jESLoggerFactory.getLogger("second");
        logger = ((Log4jESLogger) esLogger).logger();
        appender = logger.getAppender("console2");
        assertThat(appender, notNullValue());

        esLogger = Log4jESLoggerFactory.getLogger("third");
        logger = ((Log4jESLogger) esLogger).logger();
        appender = logger.getAppender("console3");
        assertThat(appender, notNullValue());
    }

    @Test
    public void testResolveJsonLoggingConfig() throws Exception {
        File tmpDir = newTempDir();
        File tmpFile = File.createTempFile("logging.", ".json", tmpDir);
        Files.write("{\"json\": \"foo\"}", tmpFile, StandardCharsets.UTF_8);
        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", tmpDir.getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("json"), is("foo"));
    }

    @Test
    public void testResolvePropertiesLoggingConfig() throws Exception {
        File tmpDir = newTempDir();
        File tmpFile = File.createTempFile("logging.", ".properties", tmpDir);
        Files.write("key: value", tmpFile, StandardCharsets.UTF_8);
        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", tmpDir.getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("key"), is("value"));
    }

    @Test
    public void testResolveConfigValidFilename() throws Exception {
        File tmpDir = newTempDir();
        File tempFileYml = File.createTempFile("logging.", ".yml", tmpDir);
        File tempFileYaml = File.createTempFile("logging.", ".yaml", tmpDir);

        Files.write("yml: bar", tempFileYml, StandardCharsets.UTF_8);
        Files.write("yaml: bar", tempFileYaml, StandardCharsets.UTF_8);
        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", tmpDir.getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), is("bar"));
        assertThat(logSettings.get("yaml"), is("bar"));
    }

    @Test
    public void testResolveConfigInvalidFilename() throws Exception {
        File tmpDir = newTempDir();
        File tempFile = File.createTempFile("logging.yml.", ".bak", tmpDir);

        Files.write("yml: bar", tempFile, StandardCharsets.UTF_8);
        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", tempFile.getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), Matchers.nullValue());
    }

    private static File resolveConfigDir() throws Exception {
        URL url = LoggingConfigurationTests.class.getResource("config");
        return new File(url.toURI());
    }
}
