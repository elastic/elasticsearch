/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.launcher;

import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.core.SuppressForbidden;

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
        String toolname = System.getProperty("cli.name", "");
        if (toolname.isBlank()) {
            String script = System.getProperty("cli.script");
            int nameStart = script.lastIndexOf(SCRIPT_PREFIX) + SCRIPT_PREFIX.length();
            toolname = script.substring(nameStart);

            if (isWindows()) {
                int dotIndex = script.indexOf(".bat"); // strip off .bat
                toolname = script.substring(0, dotIndex);
            }
        }
        String libs = System.getProperty("cli.libs", "");

        Command command = CliToolProvider.loadTool(toolname, libs).create();
        exit(command.main(args, Terminal.DEFAULT));
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").startsWith("Windows");
    }

    @SuppressForbidden(reason = "System#exit")
    private static void exit(int status) {
        System.exit(status);
    }
}
