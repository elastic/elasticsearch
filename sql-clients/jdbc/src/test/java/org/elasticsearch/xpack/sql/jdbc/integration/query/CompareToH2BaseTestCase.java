/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.query;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcAssert.assertResultSets;

public abstract class CompareToH2BaseTestCase extends ESTestCase {
    public final String queryName;
    public final String query;
    public final Integer lineNumber;
    public final Path source;

    public CompareToH2BaseTestCase(String queryName, String query, Integer lineNumber, Path source) {
        this.queryName = queryName;
        this.query = query;
        this.lineNumber = lineNumber;
        this.source = source;
    }

    protected static List<Object[]> readScriptSpec(String url) throws Exception {
        Path source = PathUtils.get(CompareToH2BaseTestCase.class.getResource(url).toURI());
        List<String> lines = Files.readAllLines(source);

        Map<String, Integer> testNames = new LinkedHashMap<>();
        List<Object[]> pairs = new ArrayList<>();

        String name = null;
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            // ignore comments
            if (!line.isEmpty() && !line.startsWith("//")) {
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
                    if (line.endsWith(";")) {
                        query.append(line.substring(0, line.length() - 1));
                    }
                    pairs.add(new Object[] { name, query.toString(), Integer.valueOf(i), source });
                    name = null;
                    query.setLength(0);
                }
            }
        }
        Assert.assertNull("Cannot find query for test " + name, name);

        return pairs;
    }

    public void testQuery() throws Throwable {
        try (Connection h2 = QuerySuite.h2Con().get();
             Connection es = QuerySuite.esCon().get()) {
            ResultSet expected, actual;
            try {
                expected = h2.createStatement().executeQuery(query);
                actual = es.createStatement().executeQuery(query);
                assertResultSets(expected, actual);
            } catch (AssertionError ae) {
                throw reworkException(new AssertionError(errorMessage(ae), ae.getCause()));
            }
        } catch (Throwable th) {
            throw reworkException(new RuntimeException(errorMessage(th)));
        }
    }

    private String errorMessage(Throwable th) {
        return format(Locale.ROOT, "test%s@%s:%d failed\n\"%s\"\n%s", queryName, source.getFileName().toString(), lineNumber, query, th.getMessage());
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), queryName, source.getFileName().toString(), lineNumber);

        th.setStackTrace(redone);
        return th;
    }
}