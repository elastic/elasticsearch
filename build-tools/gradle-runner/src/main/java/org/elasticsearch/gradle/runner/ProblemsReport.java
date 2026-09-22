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
 * Structured summary of Gradle Problems API events observed by the Tooling API runner.
 *
 * @param totalProblems total number of reported problems, including summarized duplicates
 * @param severities    counts per severity name
 * @param problems      counts per fully-qualified problem id
 */
public record ProblemsReport(int totalProblems, List<SeverityEntry> severities, List<ProblemEntry> problems) {

    public record SeverityEntry(String severity, int count) {}

    public record ProblemEntry(String id, String displayName, String severity, int count) {}

    /**
     * Writes this report as JSON to the given file.
     */
    public void writeTo(File file) throws IOException {
        file.getParentFile().mkdirs();
        try (PrintWriter writer = new PrintWriter(file, "UTF-8")) {
            writer.println("{");
            writer.printf("  \"totalProblems\" : %d,%n", totalProblems);
            writer.println("  \"severities\" : [");
            for (int i = 0; i < severities.size(); i++) {
                SeverityEntry severity = severities.get(i);
                writer.printf(
                    "    { \"severity\" : %s, \"count\" : %d }",
                    JsonStrings.jsonString(severity.severity()),
                    severity.count()
                );
                writer.println(i < severities.size() - 1 ? "," : "");
            }
            writer.println("  ],");
            writer.println("  \"problems\" : [");
            for (int i = 0; i < problems.size(); i++) {
                ProblemEntry problem = problems.get(i);
                writer.printf(
                    "    { \"id\" : %s, \"displayName\" : %s, \"severity\" : %s, \"count\" : %d }",
                    JsonStrings.jsonString(problem.id()),
                    JsonStrings.jsonString(problem.displayName()),
                    JsonStrings.jsonString(problem.severity()),
                    problem.count()
                );
                writer.println(i < problems.size() - 1 ? "," : "");
            }
            writer.println("  ]");
            writer.println("}");
        }
    }
}
