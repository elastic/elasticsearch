/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docker;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The methods in this class take a shell command and wrap it in retry logic, so that our
 * Docker builds can be more robust in the face of transient errors e.g. network issues.
 */
public class ShellRetry {
    static String loop(String name, String command) {
        return loop(name, command, 4, "exit");
    }

    static String loop(String name, String command, int indentSize, String exitKeyword) {
        String indent = " ".repeat(indentSize);

        // bash understands the `{1..10}` syntax, but other shells don't e.g. the default in Alpine Linux.
        // We therefore use an explicit sequence.
        String retrySequence = IntStream.rangeClosed(1, 10).mapToObj(String::valueOf).collect(Collectors.joining(" "));

        StringBuilder commandWithRetry = new StringBuilder("for iter in " + retrySequence + "; do \n");
        commandWithRetry.append(indent).append("  ").append(command).append(" && \n");
        commandWithRetry.append(indent).append("  exit_code=0 && break || \n");
        commandWithRetry.append(indent);
        commandWithRetry.append("    exit_code=$? && echo \"").append(name).append(" error: retry $iter in 10s\" && sleep 10; \n");
        commandWithRetry.append(indent).append("done; \n");
        commandWithRetry.append(indent).append(exitKeyword).append(" $exit_code");

        // We need to escape all newlines so that the build process doesn't run all lines onto a single line
        return commandWithRetry.toString().replaceAll(" *\n", " \\\\\n");
    }
}
