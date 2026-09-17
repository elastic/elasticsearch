/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Offline, validation-oriented view of a {@code public-<corpus>.csv-spec} file. The runtime path
 * reads specs through the shared {@code CsvSpecReader} (which strips {@code //} comments); this
 * parser exists because the <em>provenance</em> of every expected table rides in exactly those
 * comments — oracle SQL, read shape, row limits, defect blocks — and the validator and coverage
 * inventory must see them. It deliberately mirrors the shared grammar (test name line, preamble
 * directives, query terminated by {@code ;}, expected table terminated by a {@code ;} line) without
 * depending on test-framework classes, so it can run from a plain {@code JavaExec}.
 *
 * @param fileName the spec resource file name, e.g. {@code public-clickbench.csv-spec}
 * @param tests    the parsed tests, in file order
 */
public record WorkloadSpec(String fileName, List<TestSpec> tests) {

    /**
     * One parsed test and its provenance.
     *
     * @param name              full test name, including any {@code -Ignore} suffix
     * @param lineNumber        line the test name appears on
     * @param provenance        {@code // key: value} comment block immediately above the test
     * @param requiredCapabilities the {@code required_capability:} directives
     * @param datasetDirectives raw {@code dataset:} directive lines
     * @param query             the query text (directives excluded)
     * @param expectedRowCount  number of data rows in the expected table (header excluded)
     * @param expectedTable     the expected table verbatim (header line first, then data rows;
     *                          comments and warning directives excluded). Retained so paired tests
     *                          — a single-dataset query and its multi-source twin over the same
     *                          rows — can be asserted to carry identical answers offline
     */
    public record TestSpec(
        String name,
        int lineNumber,
        Map<String, String> provenance,
        List<String> requiredCapabilities,
        List<String> datasetDirectives,
        String query,
        int expectedRowCount,
        List<String> expectedTable
    ) {

        /** The test name without the {@code -Ignore} disable suffix. */
        public String baseName() {
            return disabled() ? name.substring(0, name.length() - "-Ignore".length()) : name;
        }

        /** Whether the test is disabled via the {@code -Ignore} spec-local mute mechanism. */
        public boolean disabled() {
            return name.endsWith("-Ignore");
        }

        /** The declared {@code // read-shape:} value, or null. */
        public String readShape() {
            return provenance.get("read-shape");
        }

        /** The declared {@code // max-rows:} value, or null. */
        public String maxRows() {
            return provenance.get("max-rows");
        }
    }

    /** Loads and parses a workload spec from a classpath resource. */
    public static WorkloadSpec loadFromClasspath(String resource) {
        String path = resource.startsWith("/") ? resource : "/" + resource;
        try (InputStream in = WorkloadSpec.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Workload spec resource [" + resource + "] not found on the classpath");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return parse(path.substring(1), reader.lines().toList());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read workload spec [" + resource + "]", e);
        }
    }

    /** Parses spec {@code lines}; package-private for tests. */
    public static WorkloadSpec parse(String fileName, List<String> rawLines) {
        List<TestSpec> tests = new ArrayList<>();
        Map<String, String> provenance = new LinkedHashMap<>();
        String name = null;
        int nameLine = -1;
        List<String> capabilities = new ArrayList<>();
        List<String> datasets = new ArrayList<>();
        List<String> expectedTable = new ArrayList<>();
        StringBuilder query = new StringBuilder();
        boolean inQuery = false;
        boolean inResults = false;
        int rowCount = 0;
        boolean sawHeader = false;

        for (int i = 0; i < rawLines.size(); i++) {
            String line = rawLines.get(i).trim();
            int lineNumber = i + 1;
            if (inResults) {
                if (line.startsWith(";")) {
                    tests.add(
                        new TestSpec(
                            name,
                            nameLine,
                            Map.copyOf(provenance),
                            List.copyOf(capabilities),
                            List.copyOf(datasets),
                            query.toString().trim(),
                            rowCount,
                            List.copyOf(expectedTable)
                        )
                    );
                    provenance = new LinkedHashMap<>();
                    name = null;
                    capabilities = new ArrayList<>();
                    datasets = new ArrayList<>();
                    expectedTable = new ArrayList<>();
                    query = new StringBuilder();
                    inQuery = false;
                    inResults = false;
                    rowCount = 0;
                    sawHeader = false;
                } else if (line.isEmpty() == false && line.startsWith("//") == false && line.startsWith("#") == false) {
                    if (lineIsWarningDirective(line) == false) {
                        expectedTable.add(line);
                        if (sawHeader) {
                            rowCount++;
                        } else {
                            sawHeader = true;
                        }
                    }
                }
                continue;
            }
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("//") || line.startsWith("#")) {
                if (name == null) {
                    recordProvenance(provenance, line);
                }
                continue;
            }
            if (name == null) {
                name = line.split("#", 2)[0].trim();
                nameLine = lineNumber;
                continue;
            }
            if (inQuery == false && lineIsDirective(line)) {
                String lower = line.toLowerCase(Locale.ROOT);
                if (lower.startsWith("required_capability:")) {
                    capabilities.add(line.substring("required_capability:".length()).trim());
                } else if (lower.startsWith("dataset:")) {
                    datasets.add(line);
                }
                continue;
            }
            inQuery = true;
            if (line.endsWith(";")) {
                query.append(line, 0, line.length() - 1);
                inResults = true;
            } else {
                query.append(line).append('\n');
            }
        }
        if (name != null) {
            throw new IllegalArgumentException("Test [" + name + "] has no body at the end of [" + fileName + "]");
        }
        return new WorkloadSpec(fileName, List.copyOf(tests));
    }

    private static boolean lineIsDirective(String line) {
        String lower = line.toLowerCase(Locale.ROOT);
        return lower.startsWith("required_capability:")
            || lower.startsWith("dataset:")
            || lower.startsWith("ignoreorder:")
            || lower.startsWith("pragma:")
            || lower.startsWith("request_stored:")
            || lower.startsWith("request_time_filter:");
    }

    private static boolean lineIsWarningDirective(String line) {
        String lower = line.toLowerCase(Locale.ROOT);
        return lower.startsWith("warning:") || lower.startsWith("warningregex:") || lower.startsWith("documents_found:");
    }

    /** Accumulates a {@code // key: value} comment into the pending provenance block. */
    private static void recordProvenance(Map<String, String> provenance, String commentLine) {
        String body = commentLine.startsWith("//") ? commentLine.substring(2).trim() : commentLine.substring(1).trim();
        int colon = body.indexOf(':');
        if (colon > 0) {
            String key = body.substring(0, colon).trim().toLowerCase(Locale.ROOT);
            // keys are single kebab-case words; anything with spaces is prose, not provenance
            if (key.matches("[a-z0-9-]+")) {
                provenance.put(key, body.substring(colon + 1).trim());
            }
        }
    }
}
