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

import static org.junit.jupiter.api.Assertions.assertTrue;

class ProblemsReportTests {

    @Test
    void writesJsonReport(@TempDir Path tempDir) throws Exception {
        ProblemsReport report = new ProblemsReport(
            3,
            List.of(new ProblemsReport.SeverityEntry("ERROR", 3)),
            List.of(new ProblemsReport.ProblemEntry("validation:bad\"problem", "Bad\nProblem", "ERROR", 3))
        );

        Path reportFile = tempDir.resolve("build/problems-status.json");
        report.writeTo(reportFile.toFile());

        String json = Files.readString(reportFile);
        assertTrue(json.contains("\"totalProblems\" : 3"));
        assertTrue(json.contains("\"severity\" : \"ERROR\""));
        assertTrue(json.contains("\"id\" : \"validation:bad\\\"problem\""));
        assertTrue(json.contains("\"displayName\" : \"Bad\\nProblem\""));
    }
}
