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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class EvilLoggerConfigurationTests extends ESTestCase {

    @Override
    public void tearDown() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
        super.tearDown();
    }

    public void testResolveMultipleConfigs() throws Exception {
        final Level level = ESLoggerFactory.getLogger("test").getLevel();
        try {
            final Path configDir = getDataPath("config");
            final Settings settings = Settings.builder()
                .put(Environment.PATH_CONF_SETTING.getKey(), configDir.toAbsolutePath())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            final Environment environment = new Environment(settings);
            LogConfigurator.configure(environment, true);

            {
                final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                final Configuration config = ctx.getConfiguration();
                final LoggerConfig loggerConfig = config.getLoggerConfig("test");
                final Appender appender = loggerConfig.getAppenders().get("console");
                assertThat(appender, notNullValue());
            }

            {
                final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                final Configuration config = ctx.getConfiguration();
                final LoggerConfig loggerConfig = config.getLoggerConfig("second");
                final Appender appender = loggerConfig.getAppenders().get("console2");
                assertThat(appender, notNullValue());
            }

            {
                final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                final Configuration config = ctx.getConfiguration();
                final LoggerConfig loggerConfig = config.getLoggerConfig("third");
                final Appender appender = loggerConfig.getAppenders().get("console3");
                assertThat(appender, notNullValue());
            }
        } finally {
            Configurator.setLevel("test", level);
        }
    }

    public void testDefaults() throws IOException {
        final Path configDir = getDataPath("config");
        final String level = randomFrom(Level.values()).toString();
        final Settings settings = Settings.builder()
            .put(Environment.PATH_CONF_SETTING.getKey(), configDir.toAbsolutePath())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("logger.level", level)
            .build();
        final Environment environment = new Environment(settings);
        LogConfigurator.configure(environment, true);

        final String loggerName = Loggers.commonPrefix + "test";
        final Logger logger = ESLoggerFactory.getLogger(loggerName);
        assertThat(logger.getLevel().toString(), equalTo(level));
    }

    // tests that custom settings are not overwritten by settings in the config file
    public void testResolveOrder() throws Exception {
        final Path configDir = getDataPath("config");
        final Settings settings = Settings.builder()
            .put(Environment.PATH_CONF_SETTING.getKey(), configDir.toAbsolutePath())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("logger.test_resolve_order", "TRACE")
            .build();
        final Environment environment = new Environment(settings);
        LogConfigurator.configure(environment, true);

        // args should overwrite whatever is in the config
        final String loggerName = Loggers.commonPrefix + "test_resolve_order";
        final Logger logger = ESLoggerFactory.getLogger(loggerName);
        assertTrue(logger.isTraceEnabled());
    }

}
