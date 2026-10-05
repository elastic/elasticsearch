/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class FailedTestsReportTests {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testDeserializesFullyPopulatedReport() throws Exception {
        String json = """
            {
              "successfulTasks": [":a:test", ":b:test"],
              "successfulSuites": {
                ":c:test": ["org.es.FooTest"]
              },
              "successfulTests": {
                ":c:test": ["org.es.BazTest#testQux"]
              },
              "testseed": "DEADBEEF"
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.successfulTasks(), equalTo(List.of(":a:test", ":b:test")));
        assertThat(report.successfulSuites(), equalTo(Map.of(":c:test", List.of("org.es.FooTest"))));
        assertThat(report.successfulTests(), equalTo(Map.of(":c:test", List.of("org.es.BazTest#testQux"))));
        assertThat(report.testseed(), equalTo("DEADBEEF"));
    }

    @Test
    public void testDeserializesEmptySuccessfulTasksAndTests() throws Exception {
        String json = """
            {
              "successfulTasks": [],
              "successfulSuites": {},
              "successfulTests": {},
              "testseed": "CAFE"
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.successfulTasks(), equalTo(List.of()));
        assertThat(report.successfulSuites(), equalTo(Map.of()));
        assertThat(report.successfulTests(), equalTo(Map.of()));
        assertThat(report.testseed(), equalTo("CAFE"));
    }

    @Test
    public void testDeserializesReportWithoutOptionalFields() throws Exception {
        String json = """
            {
              "testseed": "ABC"
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.successfulTasks(), equalTo(List.of()));
        assertThat(report.successfulSuites(), equalTo(Map.of()));
        assertThat(report.successfulTests(), equalTo(Map.of()));
        assertThat(report.testseed(), equalTo("ABC"));
    }

    @Test
    public void testNullFieldsDefaultToEmptyCollections() {
        FailedTestsReport report = new FailedTestsReport(null, null, null, "seed");

        assertThat(report.successfulTasks(), equalTo(List.of()));
        assertThat(report.successfulSuites(), equalTo(Map.of()));
        assertThat(report.successfulTests(), equalTo(Map.of()));
    }

    @Test
    public void testIgnoresUnknownFields() throws Exception {
        String json = """
            {
              "successfulTasks": [":a:test"],
              "unexpectedField": "ignored",
              "workUnits": []
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.successfulTasks(), equalTo(List.of(":a:test")));
    }

    @Test
    public void testNullTestseedDeserializesAsNull() throws Exception {
        String json = """
            {
              "successfulTasks": [":a:test"]
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.testseed(), is(nullValue()));
    }
}
