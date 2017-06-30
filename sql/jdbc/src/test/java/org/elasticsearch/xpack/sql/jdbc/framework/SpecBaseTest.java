/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util.framework;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.junit.Assert;
import org.junit.runners.Parameterized.Parameter;

import static java.lang.String.format;

public abstract class SpecBaseTest {

    @Parameter(0)
    public String testName;
    @Parameter(1)
    public Integer lineNumber;
    @Parameter(2)
    public Path source;

    interface Parser {
        Object parse(String line);
    }

    // returns testName, its line location, its source and the custom object (based on each test parser)
    protected static List<Object[]> readScriptSpec(String url, Parser parser) throws Exception {
        Path source = Paths.get(SpecBaseTest.class.getResource(url).toURI());
        List<String> lines = Files.readAllLines(source);

        Map<String, Integer> testNames = new LinkedHashMap<>();
        List<Object[]> pairs = new ArrayList<>();

        String name = null;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            // ignore comments
            if (!line.isEmpty() && !line.startsWith("//")) {
                // parse test name
                if (name == null) {
                    if (testNames.keySet().contains(line)) {
                        throw new IllegalStateException(format(Locale.ROOT, "Duplicate test name '%s' at line %d (previously seen at line %d)", line, i, testNames.get(line)));
                    }
                    else {
                        name = Strings.capitalize(line);
                        testNames.put(name, Integer.valueOf(i));
                    }
                }
                else {
                    Object result = parser.parse(line);
                    // only if the parser is ready, add the object - otherwise keep on serving it lines
                    if (result != null) {
                        pairs.add(new Object[] { name, Integer.valueOf(i), source, result });
                        name = null;
                    }
                }
            }
        }
        Assert.assertNull("Cannot find spec for test " + name, name);

        return pairs;
    }
    
    Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), testName, source.getFileName().toString(), lineNumber);

        th.setStackTrace(redone);
        return th;
    }
}
