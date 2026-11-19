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

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    public record TaskEntry(String path, String outcome) {}

    public record SuiteEntry(String taskPath, String className, String result) {}

    public record TestEntry(String taskPath, String className, String methodName, String result) {}

    /**
     * Writes this report as JSON to the given file.
     */
    public void writeTo(File file) throws IOException {
        file.getParentFile().mkdirs();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(file, JsonEncoding.UTF8)) {
            generator.useDefaultPrettyPrinter();
            generator.writeStartObject();
            generator.writeArrayFieldStart("tasks");
            for (TaskEntry task : tasks) {
                generator.writeStartObject();
                generator.writeStringField("path", task.path());
                generator.writeStringField("outcome", task.outcome());
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeArrayFieldStart("suites");
            for (SuiteEntry suite : suites) {
                generator.writeStartObject();
                generator.writeStringField("taskPath", suite.taskPath());
                generator.writeStringField("className", suite.className());
                generator.writeStringField("result", suite.result());
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeArrayFieldStart("tests");
            for (TestEntry test : tests) {
                generator.writeStartObject();
                generator.writeStringField("taskPath", test.taskPath());
                generator.writeStringField("className", test.className());
                generator.writeStringField("methodName", test.methodName());
                generator.writeStringField("result", test.result());
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeBooleanField("cancelled", cancelled);
            if (preemptedAt == null) {
                generator.writeNullField("preemptedAt");
            } else {
                generator.writeStringField("preemptedAt", preemptedAt);
            }
            generator.writeEndObject();
            generator.writeRaw(System.lineSeparator());
        }
    }
}
