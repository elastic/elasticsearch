/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.launcher;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * A unified main method for Elasticsearch tools.
 *
 * This class should only be called by the elasticsearch-cli script.
 */
class CliToolLauncher {
    private static final String SCRIPT_PREFIX = "elasticsearch-";

    /**
     * Runs a CLI tool.
     *
     * <p>
     * The following system properties may be provided:
     * <ul>
     *     <li><code>cli.name</code> - the name of the tool to run</li>
     *     <li><code>cli.script</code> - the path to the script that is being run</li>
     *     <li><code>cli.libs</code> - optional, comma separated list of additional lib directories
     *                                 that should be loaded to find the tool</li>
     * </ul>
     * One of either <code>cli.name</code> or <code>cli.script</code> must be provided.
     *
     * @param args args to the tool
     * @throws Exception if the tool fails with an unknown error
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> sysprops = getSystemProperties();

        // configure logging as early as possible
        configureLoggingWithoutConfig(sysprops);

        String toolname = getToolName(sysprops);
        String libs = sysprops.getOrDefault("cli.libs", "");

        Command command = CliToolProvider.load(toolname, libs).create();
        exit(command.main(args, Terminal.DEFAULT));
    }

    // package private for tests
    static String getToolName(Map<String, String> sysprops) {
        String toolname = sysprops.getOrDefault("cli.name", "");
        if (toolname.isBlank()) {
            String script = sysprops.get("cli.script");
            int nameStart = script.lastIndexOf(SCRIPT_PREFIX) + SCRIPT_PREFIX.length();
            toolname = script.substring(nameStart);

            if (sysprops.get("os.name").startsWith("Windows")) {
                int dotIndex = toolname.indexOf(".bat"); // strip off .bat
                toolname = toolname.substring(0, dotIndex);
            }
        }
        return toolname;
    }

    @SuppressForbidden(reason = "collect system properties")
    private static Map<String, String> getSystemProperties() {
        Properties props = System.getProperties();
        return props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    @SuppressForbidden(reason = "System#exit")
    private static void exit(int status) {
        System.exit(status);
    }

    /**
     * Configures logging without Elasticsearch configuration files based on the system property "es.logger.level" only. As such, any
     * logging will be written to the console.
     */
    private static void configureLoggingWithoutConfig(Map<String, String> sysprops) {
        // initialize default for es.logger.level because we will not read the log4j2.properties
        final String loggerLevel = sysprops.getOrDefault("es.logger.level", Level.INFO.name());
        final Settings settings = Settings.builder().put("logger.level", loggerLevel).build();
        LogConfigurator.configureWithoutConfig(settings);
    }
}
