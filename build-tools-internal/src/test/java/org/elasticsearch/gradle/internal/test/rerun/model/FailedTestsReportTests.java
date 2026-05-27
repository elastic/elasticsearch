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
              "workUnits": [],
              "testseed": "DEADBEEF",
              "executedTestTasks": [":a:test", ":b:test"],
              "failedTestTasks": [":b:test"]
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.workUnits(), equalTo(List.of()));
        assertThat(report.testseed(), equalTo("DEADBEEF"));
        assertThat(report.executedTestTasks(), equalTo(List.of(":a:test", ":b:test")));
        assertThat(report.failedTestTasks(), equalTo(List.of(":b:test")));
    }

    @Test
    public void testDeserializesLegacyReportWithoutFailedTestTasks() throws Exception {
        // Back-compat: a history JSON written before the failedTestTasks field existed
        // must still deserialize, with failedTestTasks left null for safe fallback.
        String json = """
            {
              "workUnits": [],
              "testseed": "CAFE",
              "executedTestTasks": [":a:test"]
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.executedTestTasks(), equalTo(List.of(":a:test")));
        assertThat(report.failedTestTasks(), is(nullValue()));
    }

    @Test
    public void testDeserializesReportWithoutExecutedTestTasks() throws Exception {
        // Older format where executedTestTasks was not yet emitted.
        String json = """
            {
              "workUnits": []
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.executedTestTasks(), is(nullValue()));
        assertThat(report.failedTestTasks(), is(nullValue()));
    }

    @Test
    public void testEmptyArraysAreDistinctFromNull() throws Exception {
        // Empty arrays mean "API reachable but returned no entries" — not the same as
        // "API data unavailable" (null). The plugin treats them differently.
        String json = """
            {
              "workUnits": [],
              "executedTestTasks": [],
              "failedTestTasks": []
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.executedTestTasks(), equalTo(List.of()));
        assertThat(report.failedTestTasks(), equalTo(List.of()));
    }

    @Test
    public void testNullWorkUnitsDefaultsToEmptyList() {
        // The compact constructor must normalise a null workUnits argument to an empty
        // list so that consumers can iterate it unconditionally.
        FailedTestsReport report = new FailedTestsReport(null, "seed", null, null);

        assertThat(report.workUnits(), equalTo(List.of()));
        assertThat(report.executedTestTasks(), is(nullValue()));
        assertThat(report.failedTestTasks(), is(nullValue()));
    }

    @Test
    public void testIgnoresUnknownFields() throws Exception {
        String json = """
            {
              "workUnits": [],
              "unexpectedField": "ignored"
            }
            """;

        FailedTestsReport report = objectMapper.readValue(json, FailedTestsReport.class);

        assertThat(report.workUnits(), equalTo(List.of()));
    }
}
