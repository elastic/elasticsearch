/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class EvilLoggerConfigurationTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LogConfigurator.registerErrorListener();
    }

    @Override
    public void tearDown() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
        super.tearDown();
    }

    public void testResolveMultipleConfigs() throws Exception {
        final Level level = LogManager.getLogger("test").getLevel();
        try {
            final Path configDir = getDataPath("config");
            final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
            final Environment environment = new Environment(settings, configDir);
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

    public void testDefaults() throws IOException, UserException {
        final Path configDir = getDataPath("config");
        final String level = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR).toString();
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("logger.level", level)
            .build();
        final Environment environment = new Environment(settings, configDir);
        LogConfigurator.configure(environment, true);

        final String loggerName = "test";
        final Logger logger = LogManager.getLogger(loggerName);
        assertThat(logger.getLevel().toString(), equalTo(level));
    }

    // tests that custom settings are not overwritten by settings in the config file
    public void testResolveOrder() throws Exception {
        final Path configDir = getDataPath("config");
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("logger.test_resolve_order", "TRACE")
            .build();
        final Environment environment = new Environment(settings, configDir);
        LogConfigurator.configure(environment, true);

        // args should overwrite whatever is in the config
        final String loggerName = "test_resolve_order";
        final Logger logger = LogManager.getLogger(loggerName);
        assertTrue(logger.isTraceEnabled());
    }

    public void testHierarchy() throws Exception {
        final Path configDir = getDataPath("hierarchy");
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        final Environment environment = new Environment(settings, configDir);
        LogConfigurator.configure(environment, true);

        assertThat(LogManager.getLogger("x").getLevel(), equalTo(Level.TRACE));
        assertThat(LogManager.getLogger("x.y").getLevel(), equalTo(Level.DEBUG));

        final Level level = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        Loggers.setLevel(LogManager.getLogger("x"), level);

        assertThat(LogManager.getLogger("x").getLevel(), equalTo(level));
        assertThat(LogManager.getLogger("x.y").getLevel(), equalTo(level));
    }

    public void testLoggingLevelsFromSettings() throws IOException, UserException {
        final Level rootLevel = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        final Level fooLevel = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        final Level barLevel = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        final Path configDir = getDataPath("minimal");
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("logger.level", rootLevel.name())
            .put("logger.foo", fooLevel.name())
            .put("logger.bar", barLevel.name())
            .build();
        final Environment environment = new Environment(settings, configDir);
        LogConfigurator.configure(environment, true);

        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        final Map<String, LoggerConfig> loggerConfigs = config.getLoggers();
        assertThat(loggerConfigs.size(), equalTo(5));
        assertThat(loggerConfigs, hasKey(""));
        assertThat(loggerConfigs.get("").getLevel(), equalTo(rootLevel));
        assertThat(loggerConfigs, hasKey("foo"));
        assertThat(loggerConfigs.get("foo").getLevel(), equalTo(fooLevel));
        assertThat(loggerConfigs, hasKey("bar"));
        assertThat(loggerConfigs.get("bar").getLevel(), equalTo(barLevel));

        assertThat(ctx.getLogger(randomAlphaOfLength(16)).getLevel(), equalTo(rootLevel));
    }

}
