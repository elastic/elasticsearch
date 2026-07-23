/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * The task and test status report written to {@code task-status.json} at the end of every build.
 * Format-compatible with the Gradle-internal {@code TaskStatusReport} so downstream CI tooling
 * can consume either source interchangeably.
 *
 * <p>The report uses a hierarchical inclusion strategy to keep file size small:
 * <ul>
 *   <li>Successful tasks: suites and tests omitted entirely</li>
 *   <li>Unsuccessful tasks: suites included; if a suite succeeded, its individual tests are omitted</li>
 *   <li>Unsuccessful suites: individual test methods included</li>
 * </ul>
 *
 * @param tasks       every task in the execution graph, sorted by path, with its final outcome
 * @param suites      test class (suite) results for tasks that did not complete successfully
 * @param tests       individual test methods for suites that did not complete successfully
 * @param cancelled   {@code true} when the build was explicitly cancelled (preemption signal)
 * @param preemptedAt ISO-8601 timestamp of when GCP preemption was detected, or {@code null}
 */
public record StatusReport(List<TaskEntry> tasks, List<SuiteEntry> suites, List<TestEntry> tests, boolean cancelled, String preemptedAt) {

    public record TaskEntry(String path, String outcome) {}

    public record SuiteEntry(String taskPath, String className, String result) {}

    public record TestEntry(String taskPath, String className, String methodName, String result) {}

    /**
     * Writes this report as JSON to the given file. Uses simple string building to avoid
     * requiring Jackson or other JSON libraries in the fat JAR.
     */
    public void writeTo(File file) throws IOException {
        file.getParentFile().mkdirs();
        try (PrintWriter w = new PrintWriter(file, "UTF-8")) {
            w.println("{");
            w.println("  \"tasks\" : [");
            for (int i = 0; i < tasks.size(); i++) {
                TaskEntry t = tasks.get(i);
                w.printf("    { \"path\" : %s, \"outcome\" : %s }", jsonString(t.path()), jsonString(t.outcome()));
                w.println(i < tasks.size() - 1 ? "," : "");
            }
            w.println("  ],");
            w.println("  \"suites\" : [");
            for (int i = 0; i < suites.size(); i++) {
                SuiteEntry s = suites.get(i);
                w.printf(
                    "    { \"taskPath\" : %s, \"className\" : %s, \"result\" : %s }",
                    jsonString(s.taskPath()),
                    jsonString(s.className()),
                    jsonString(s.result())
                );
                w.println(i < suites.size() - 1 ? "," : "");
            }
            w.println("  ],");
            w.println("  \"tests\" : [");
            for (int i = 0; i < tests.size(); i++) {
                TestEntry t = tests.get(i);
                w.printf(
                    "    { \"taskPath\" : %s, \"className\" : %s, \"methodName\" : %s, \"result\" : %s }",
                    jsonString(t.taskPath()),
                    jsonString(t.className()),
                    jsonString(t.methodName()),
                    jsonString(t.result())
                );
                w.println(i < tests.size() - 1 ? "," : "");
            }
            w.println("  ],");
            w.printf("  \"cancelled\" : %s,%n", cancelled);
            w.printf("  \"preemptedAt\" : %s%n", preemptedAt != null ? jsonString(preemptedAt) : "null");
            w.println("}");
        }
    }

    private static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
