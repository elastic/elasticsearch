/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.docker;

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

        StringBuilder commandWithRetry = new StringBuilder("for iter in {1..10}; do \n");
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
