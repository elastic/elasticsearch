/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Structured summary of Gradle Problems API events observed by the Tooling API runner.
 *
 * @param totalProblems total number of reported problems, including summarized duplicates
 * @param severities    counts per severity name
 * @param problems      counts per fully-qualified problem id
 */
public record ProblemsReport(int totalProblems, List<SeverityEntry> severities, List<ProblemEntry> problems) {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    public record SeverityEntry(String severity, int count) {}

    public record ProblemEntry(String id, String displayName, String severity, int count) {}

    /**
     * Writes this report as JSON to the given file.
     */
    public void writeTo(File file) throws IOException {
        file.getParentFile().mkdirs();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(file, JsonEncoding.UTF8)) {
            generator.useDefaultPrettyPrinter();
            generator.writeStartObject();
            generator.writeNumberField("totalProblems", totalProblems);
            generator.writeArrayFieldStart("severities");
            for (SeverityEntry severity : severities) {
                generator.writeStartObject();
                generator.writeStringField("severity", severity.severity());
                generator.writeNumberField("count", severity.count());
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeArrayFieldStart("problems");
            for (ProblemEntry problem : problems) {
                generator.writeStartObject();
                generator.writeStringField("id", problem.id());
                generator.writeStringField("displayName", problem.displayName());
                generator.writeStringField("severity", problem.severity());
                generator.writeNumberField("count", problem.count());
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.writeRaw(System.lineSeparator());
        }
    }
}
