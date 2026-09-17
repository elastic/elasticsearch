/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.GenerativeFeature;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests the section ordering and the null handling of {@link GenerativeRestTest#failureReport}.
 */
public class GenerativeFailureReportTests extends ESTestCase {

    private static final String QUERY = "FROM employees | EVAL x = 1 / 0 | KEEP x";

    private static final String WARNINGS = "Warnings: [Line 1:20: evaluation of [1 / 0] failed, treating result as null, "
        + "Line 1:20: java.lang.ArithmeticException: / by zero]";

    private static final String BODY = "{\"error\":{\"type\":\"verification_exception\",\"reason\":\"boom\"}}";

    private static final String EXTRA_CONTEXT = "\n=== Reproduction commands ===\nPUT /gen_rm_0\n";

    /** The section labels {@link GenerativeRestTest#failureReport} emits, in the order the report should carry them. */
    private static final List<String> SECTION_LABELS = List.of(
        "query: ",
        "features: ",
        "reproduce with ",
        "error: ",
        "=== Reproduction commands ===",
        "Warnings: ["
    );

    /**
     * The message {@code ResponseException} builds: a request line, an optional {@code Warnings} block, then the
     * response body.
     */
    private static String responseExceptionMessage(@Nullable String warnings) {
        return "method [POST], host [http://[::1]:9200], URI [/_query], status line [HTTP/1.1 400 Bad Request]"
            + (warnings == null ? "" : "\n" + warnings)
            + "\n"
            + BODY;
    }

    private static List<String> sectionsOf(String report) {
        List<String> sections = new ArrayList<>();
        for (String line : report.split("\n", -1)) {
            for (String label : SECTION_LABELS) {
                if (line.startsWith(label)) {
                    sections.add(label);
                }
            }
        }
        return sections;
    }

    private static GenerativeRestTest reporter() {
        return reporter("");
    }

    private static GenerativeRestTest reporter(String extraContext) {
        return new GenerativeRestTest() {
            @Override
            protected boolean supportsSourceFieldMapping() {
                throw new AssertionError("no cluster is started for these tests");
            }

            @Override
            protected Set<GenerativeFeature> enabledFeatures() {
                return Set.of(GenerativeFeature.SUBQUERIES);
            }

            @Override
            protected String extraFailureContext(@Nullable String query) {
                return extraContext;
            }
        };
    }

    public void testWarningsMoveBelowTheError() {
        String report = reporter().failureReport(QUERY, responseExceptionMessage(WARNINGS));

        assertThat(sectionsOf(report), contains("query: ", "features: ", "reproduce with ", "error: ", "Warnings: ["));
        assertThat(report, containsString(BODY));
        assertThat(report, endsWith(WARNINGS));
    }

    public void testExtraContextGoesBetweenTheErrorAndTheWarnings() {
        String report = reporter(EXTRA_CONTEXT).failureReport(QUERY, responseExceptionMessage(WARNINGS));

        assertThat(
            sectionsOf(report),
            contains("query: ", "features: ", "reproduce with ", "error: ", "=== Reproduction commands ===", "Warnings: [")
        );
    }

    public void testErrorWithoutWarningsIsLeftIntact() {
        String error = responseExceptionMessage(null);
        String report = reporter().failureReport(QUERY, error);

        assertThat(report, endsWith("\nerror: " + error));
        assertThat(report, not(containsString("Warnings: ")));
    }

    public void testNullErrorDoesNotThrow() {
        assertThat(reporter().failureReport(QUERY, null), endsWith("\nerror: <no error message>"));
    }

    public void testNullQueryDoesNotThrow() {
        String report = reporter().failureReport(null, "boom");

        assertThat(report, startsWith("query: <no query generated>\n"));
        assertThat(report, endsWith("\nerror: boom"));
    }

    public void testNullQueryAndErrorDoNotThrow() {
        String report = reporter().failureReport(null, null);

        assertThat(report, startsWith("query: <no query generated>\n"));
        assertThat(report, endsWith("\nerror: <no error message>"));
    }
}
