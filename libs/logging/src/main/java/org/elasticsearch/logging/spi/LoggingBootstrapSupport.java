/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;

import org.elasticsearch.logging.Level;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Used on startup and in testing infra. We can considere limiting the scope of export
 */
public interface LoggingBootstrapSupport {
    static LoggingBootstrapSupport provider() {
        return LoggingSupportProvider.provider().loggingBootstrapSupport();
    }

    /**
     * Registers a listener for status logger errors. This listener should be registered as early as possible to ensure that no errors are
     * logged by the status logger before logging is configured.
     */
    void registerErrorListener();

    /**
     * Configure logging without reading a log4j2.properties file, effectively configuring the
     * status logger and all loggers to the console.
     * <p>
     * //* @param settings for configuring logger.level and individual loggers
     */
    void configureWithoutConfig(Optional<Level> defaultLogLevel, Map<String, Level> logLevelSettingsMap);

    /**
     * Configure logging reading from any log4j2.properties found in the config directory and its
     * subdirectories from the specified environment. Will also configure logging to point the logs
     * directory from the specified environment.
     * <p>
     * //* @param environment the environment for reading configs and the logs path
     *
     * @throws IOException   if there is an issue readings any log4j2.properties in the config
     *                       directory
     * @gthrows UserException if there are no log4j2.properties in the specified configs path
     */
    void configure(
        String clusterName,
        String nodeName,
        Optional<Level> defaultLogLevel,
        Map<String, Level> logLevelSettingsMap,
        Path configFile,
        Path logsFile
    ) throws IOException;

    /**
     * Load logging plugins so we can have {@code node_name} in the pattern.
     */
    void loadLog4jPlugins();

    /**
     * Sets the node name. This is called before logging is configured if the
     * node name is set in elasticsearch.yml. Otherwise it is called as soon
     * as the node id is available.
     */
    void setNodeName(String nodeName);

    void init();

    void shutdown();

    Consumer<ConsoleAppenderMode> consoleAppender();

    /* TODO PG private */
    void checkErrorListener();

    enum ConsoleAppenderMode {
        ENABLE,
        DISABLE
    }
}
