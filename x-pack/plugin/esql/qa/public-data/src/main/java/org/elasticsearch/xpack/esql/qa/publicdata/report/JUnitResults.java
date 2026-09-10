/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.report;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Minimal reader of the JUnit XML files Gradle writes for {@code publicDataTest} runs — the
 * per-shard artifacts the merge step feeds into the verdict. Hand-rolled tag scanning, same
 * posture as the S3 listing parser: the schema is fixed, flat where we look, and the verdict CLI
 * must run from a bare {@code JavaExec}.
 */
public final class JUnitResults {

    /** One observed test execution. */
    public record TestResult(String variantLabel, String testName, Status status, String failureMessage) {}

    public enum Status {
        PASSED,
        FAILED,
        SKIPPED
    }

    /** {@code test {public-data:<file>.<test>{<variant>}}} — the workload IT naming. */
    private static final Pattern WORKLOAD_NAME = Pattern.compile("test \\{public-data:[^.]+\\.([^{]+)\\{([^}]+)}}");
    /** {@code testFailsCleanly {<variant>}} — the failure IT naming. */
    private static final Pattern FAILURE_NAME = Pattern.compile("testFailsCleanly \\{([^}]+)}");
    private static final Pattern TESTCASE = Pattern.compile("<testcase name=\"([^\"]+)\"[^>]*?(/>|>)");

    private JUnitResults() {}

    /** Parses every {@code TEST-*.xml} under {@code resultsDir} (recursively). */
    public static List<TestResult> parse(Path resultsDir) throws IOException {
        List<TestResult> results = new ArrayList<>();
        try (Stream<Path> files = Files.walk(resultsDir)) {
            for (Path file : files.filter(f -> {
                String name = f.getFileName().toString();
                return name.startsWith("TEST-") && name.endsWith(".xml");
            }).sorted().toList()) {
                parseFile(Files.readString(file, StandardCharsets.UTF_8), results);
            }
        }
        return results;
    }

    static void parseFile(String xml, List<TestResult> results) {
        Matcher testcase = TESTCASE.matcher(xml);
        while (testcase.find()) {
            String rawName = unescape(testcase.group(1));
            String variant = null;
            String test = null;
            Matcher workload = WORKLOAD_NAME.matcher(rawName);
            Matcher failure = FAILURE_NAME.matcher(rawName);
            if (workload.find()) {
                test = workload.group(1);
                variant = workload.group(2);
            } else if (failure.find()) {
                test = "testFailsCleanly";
                variant = failure.group(1);
            } else {
                continue; // not a public-data test (e.g. an inherited-factory leftover)
            }
            Status status = Status.PASSED;
            String message = null;
            if (testcase.group(2).equals(">")) {
                // element has a body: look at it up to the closing tag for failure/skipped children
                int bodyStart = testcase.end();
                int bodyEnd = xml.indexOf("</testcase>", bodyStart);
                String body = xml.substring(bodyStart, bodyEnd < 0 ? xml.length() : bodyEnd);
                if (body.contains("<failure") || body.contains("<error")) {
                    status = Status.FAILED;
                    Matcher msg = Pattern.compile("<(?:failure|error)[^>]*message=\"([^\"]{0,500})").matcher(body);
                    message = msg.find() ? unescape(msg.group(1)) : "";
                } else if (body.contains("<skipped")) {
                    status = Status.SKIPPED;
                }
            }
            results.add(new TestResult(variant, test, status, message));
        }
    }

    private static String unescape(String value) {
        return value.replace("&#10;", "\n")
            .replace("&quot;", "\"")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&apos;", "'")
            .replace("&amp;", "&");
    }
}
