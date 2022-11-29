/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.RequestObjectBuilder;
import org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.ql.SpecReader;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.ql.TestUtils.classpathResources;

public abstract class EsqlSpecTestCase extends ESRestTestCase {

    private static final CsvPreference CSV_SPEC_PREFERENCES = new CsvPreference.Builder('"', '|', "\r\n").build();

    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    private final CsvTestCase testCase;

    @ParametersFactory(argumentFormatting = "%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        return SpecReader.readScriptSpec(urls, specParser());
    }

    public EsqlSpecTestCase(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
    }

    public final void test() throws Throwable {
        try {
            assumeFalse("Test " + testName + " is not enabled", testName.endsWith("-Ignore"));
            doTest();
        } catch (Exception e) {
            throw reworkException(e);
        }
    }

    protected final void doTest() throws Throwable {
        RequestObjectBuilder builder = new RequestObjectBuilder(randomFrom(XContentType.values()));
        Map<String, Object> answer = runEsql(builder.query(testCase.query).build());

        var expectedColumnsWithValues = expectedColumnsWithValues(testCase.expectedResults);

        assertNotNull(answer.get("columns"));
        @SuppressWarnings("unchecked")
        List<Map<String, String>> actualColumns = (List<Map<String, String>>) answer.get("columns");
        assertColumns(expectedColumnsWithValues.v1(), actualColumns);

        assertNotNull(answer.get("values"));
        @SuppressWarnings("unchecked")
        List<List<Object>> actualValues = (List<List<Object>>) answer.get("values");
        assertValues(expectedColumnsWithValues.v2(), actualValues);
    }

    private void assertColumns(List<Tuple<String, String>> expectedColumns, List<Map<String, String>> actualColumns) {
        assertEquals("Unexpected number of columns in " + actualColumns, expectedColumns.size(), actualColumns.size());

        for (int i = 0; i < expectedColumns.size(); i++) {
            assertEquals(expectedColumns.get(i).v1(), actualColumns.get(i).get("name"));
            String expectedType = expectedColumns.get(i).v2();
            if (expectedType != null) {
                assertEquals(expectedType, actualColumns.get(i).get("type"));
            }
        }
    }

    private void assertValues(List<List<String>> expectedValues, List<List<Object>> actualValues) {
        assertEquals("Unexpected number of columns in " + actualValues, expectedValues.size(), actualValues.size());

        for (int i = 0; i < expectedValues.size(); i++) {
            assertEquals(
                expectedValues.get(i),
                actualValues.get(i).stream().map(o -> { return o == null ? "null" : o.toString(); }).toList()
            );
        }
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    private Tuple<List<Tuple<String, String>>, List<List<String>>> expectedColumnsWithValues(String csv) {
        try (CsvListReader listReader = new CsvListReader(new StringReader(csv), CSV_SPEC_PREFERENCES)) {
            String[] header = listReader.getHeader(true);
            List<Tuple<String, String>> columns = Arrays.stream(header).map(c -> {
                String[] nameWithType = c.split(":");
                String name = nameWithType[0].trim();
                String type = nameWithType.length > 1 ? nameWithType[1].trim() : null;
                return Tuple.tuple(name, type);
            }).toList();

            List<List<String>> values = new LinkedList<>();

            List<String> row;
            while ((row = listReader.read()) != null) {
                values.add(row.stream().map(String::trim).toList());
            }

            return Tuple.tuple(columns, values);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
