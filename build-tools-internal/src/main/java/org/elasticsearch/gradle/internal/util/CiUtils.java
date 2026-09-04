/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.util;

import groovy.lang.Closure;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CiUtils {

    private static final Logger logger = Logging.getLogger(CiUtils.class);

    public static String safeName(String input) {
        return input.replaceAll("[^a-zA-Z0-9_\\-\\.]+", " ").trim().replaceAll(" ", "_").toLowerCase();
    }

    /**
     * Executes a buildkite-agent command with timeout and exit code checking.
     * Failures are logged as warnings but do not fail the build.
     */
    public static void runBuildkiteAgent(List<String> args, String description, Closure<?> stdinWriter) {
        try {
            List<String> command = new ArrayList<>();
            command.add("buildkite-agent");
            command.addAll(args);
            Process process = new ProcessBuilder(command).start();
            if (stdinWriter != null) {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()))) {
                    stdinWriter.call(writer);
                }
            }
            boolean completed = process.waitFor(30, TimeUnit.SECONDS);
            if (completed == false) {
                logger.warn("Timeout {}", description);
                process.destroyForcibly();
            } else if (process.exitValue() != 0) {
                logger.warn("Failed {}: exit code {}", description, process.exitValue());
            }
        } catch (Exception e) {
            logger.warn("Failed {}: {}", description, e.getMessage());
        }
    }

    /**
     * Executes a buildkite-agent command with timeout and exit code checking.
     * Failures are logged as warnings but do not fail the build.
     */
    public static void runBuildkiteAgent(List<String> args, String description) {
        runBuildkiteAgent(args, description, null);
    }
}
