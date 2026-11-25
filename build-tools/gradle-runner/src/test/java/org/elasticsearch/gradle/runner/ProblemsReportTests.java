/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProblemsReportTests {

    @Test
    void writesJsonReport(@TempDir Path tempDir) throws Exception {
        ProblemsReport report = new ProblemsReport(
            3,
            List.of(new ProblemsReport.SeverityEntry("ERROR", 3)),
            List.of(
                new ProblemsReport.ProblemEntry(
                    "Gradle:bad\"problem",
                    "Bad\nProblem",
                    "ERROR",
                    3,
                    List.of(new ProblemsReport.LabelEntry("Task ':a' \"x\"", 2))
                ),
                new ProblemsReport.ProblemEntry("Gradle:no-labels", "No labels", "ERROR", 1, List.of())
            )
        );

        Path reportFile = tempDir.resolve("build/problems-status.json");
        report.writeTo(reportFile.toFile());

        String json = Files.readString(reportFile);
        assertTrue(json.contains("\"totalProblems\" : 3"));
        assertTrue(json.contains("\"severity\" : \"ERROR\""));
        assertTrue(json.contains("\"id\" : \"Gradle:bad\\\"problem\""));
        assertTrue(json.contains("\"displayName\" : \"Bad\\nProblem\""));
        assertTrue(json.contains("\"label\" : \"Task ':a' \\\"x\\\"\""));
        // the labels array is omitted for entries without labels
        assertEquals(1, json.split("\"labels\"", -1).length - 1);
    }
}
