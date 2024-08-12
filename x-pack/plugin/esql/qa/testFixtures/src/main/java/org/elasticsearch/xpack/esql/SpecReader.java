/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.Strings;

import java.io.BufferedReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertNull;

public final class SpecReader {

    private SpecReader() {}

    public static List<Object[]> readScriptSpec(URL source, String url, Parser parser) throws Exception {
        Objects.requireNonNull(source, "Cannot find resource " + url);
        return readURLSpec(source, parser);
    }

    public static List<Object[]> readScriptSpec(List<URL> urls, Parser parser) throws Exception {
        List<Object[]> results = emptyList();
        for (URL url : urls) {
            List<Object[]> specs = readURLSpec(url, parser);
            if (results.isEmpty()) {
                results = specs;
            } else {
                results.addAll(specs);
            }
        }

        return results;
    }

    public static List<Object[]> readURLSpec(URL source, Parser parser) throws Exception {
        String fileName = EsqlTestUtils.pathAndName(source.getFile()).v2();
        String groupName = fileName.substring(0, fileName.lastIndexOf('.'));

        Map<String, Integer> testNames = new LinkedHashMap<>();
        List<Object[]> testCases = new ArrayList<>();

        String testName = null;
        try (BufferedReader reader = EsqlTestUtils.reader(source)) {
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (shouldSkipLine(line) == false) {
                    // parse test name
                    if (testName == null) {
                        if (testNames.keySet().contains(line)) {
                            throw new IllegalStateException(
                                "Duplicate test name '"
                                    + line
                                    + "' at line "
                                    + lineNumber
                                    + " (previously seen at line "
                                    + testNames.get(line)
                                    + ")"
                            );
                        } else {
                            testName = Strings.capitalize(line);
                            testNames.put(testName, Integer.valueOf(lineNumber));
                        }
                    } else {
                        Object result = parser.parse(line);
                        // only if the parser is ready, add the object - otherwise keep on serving it lines
                        if (result != null) {
                            testCases.add(makeTestCase(fileName, groupName, testName, lineNumber, result));
                            testName = null;
                        }
                    }
                }
                lineNumber++;
            }
            if (testName != null) {
                throw new IllegalStateException("Read a test without a body at the end of [" + fileName + "].");
            }
        }
        assertNull("Cannot find spec for test " + testName, testName);

        return testCases;
    }

    public interface Parser {
        Object parse(String line);
    }

    public static boolean shouldSkipLine(String line) {
        return line.isEmpty() || line.startsWith("//") || line.startsWith("#");
    }

    private static Object[] makeTestCase(String fileName, String groupName, String testName, int lineNumber, Object result) {
        var testNameParts = testName.split("#", 2);

        testName = testNameParts[0];
        var instructions = testNameParts.length == 2 ? testNameParts[1] : "";

        return new Object[] { fileName, groupName, testName, lineNumber, result, instructions };
    }
}
