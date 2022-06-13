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
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * A unified main method for Elasticsearch tools.
 *
 * This class should only be called by the elasticsearch-cli script.
 */
class CliToolLauncher {
    private static final String SCRIPT_PREFIX = "elasticsearch-";

    private static volatile Command command;

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
        ProcessInfo pinfo = ProcessInfo.fromSystem();

        // configure logging as early as possible
        configureLoggingWithoutConfig(pinfo.sysprops());

        String toolname = getToolName(pinfo.sysprops());
        String libs = pinfo.sysprops().getOrDefault("cli.libs", "");

        command = CliToolProvider.load(toolname, libs).create();
        Terminal terminal = Terminal.DEFAULT;
        Runtime.getRuntime().addShutdownHook(createShutdownHook(terminal, command));

        int exitCode = command.main(args, terminal, pinfo);
        terminal.flush(); // make sure nothing is left in buffers
        if (exitCode != ExitCodes.OK) {
            exit(exitCode);
        }
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
                if (dotIndex != -1) {
                    toolname = toolname.substring(0, dotIndex);
                }
            }
        }
        return toolname;
    }

    static Thread createShutdownHook(Terminal terminal, Closeable closeable) {
        return new Thread(() -> {
            try {
                closeable.close();
            } catch (final IOException e) {
                e.printStackTrace(terminal.getErrorWriter());
            }
            terminal.flush(); // make sure to flush whatever the close or error might have written
        });

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

    /**
      * Required method that's called by Apache Commons procrun when
      * running as a service on Windows, when the service is stopped.
      *
      * http://commons.apache.org/proper/commons-daemon/procrun.html
      *
      * NOTE: If this method is renamed and/or moved, make sure to
      * update WindowsServiceInstallCommand!
      */
    static void close(String[] args) throws IOException {
        command.close();
    }
}
