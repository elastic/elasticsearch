/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.jdbc.framework.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;

/**
 * Tests comparing sql queries executed against our jdbc client
 * with hard coded result sets.
 */
public class CsvSpecIT extends SpecBaseIntegrationTestCase {
    private final CsvTestCase testCase;

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTNG) // NOCOMMIT are we sure?!
    public static List<Object[]> readScriptSpec() throws Exception {
        CsvSpecParser parser = new CsvSpecParser();
        return CollectionUtils.combine(
                readScriptSpec("/command.csv-spec", parser),
                readScriptSpec("/fulltext.csv-spec", parser));
    }

    public CsvSpecIT(String groupName, String testName, Integer lineNumber, Path source, CsvTestCase testCase) {
        super(groupName, testName, lineNumber, source);
        this.testCase = testCase;
    }

    public void test() throws Throwable {
        try {
            assertMatchesCsv(testCase.query, testName, testCase.expectedResults);            
        } catch (AssertionError ae) {
            throw reworkException(new AssertionError(errorMessage(ae), ae.getCause()));
        } catch (Throwable th) {
            throw reworkException(th);
        }
    }

    String errorMessage(Throwable th) {
        return format(Locale.ROOT, "test%s@%s:%d failed\n\"%s\"\n%s", testName, source.getFileName().toString(), lineNumber,
                testCase.query, th.getMessage());
    }

    private static class CsvSpecParser implements Parser {
        private final StringBuilder data = new StringBuilder();
        private CsvTestCase testCase;

        @Override
        public Object parse(String line) {
            // beginning of the section
            if (testCase == null) {
                // pick up the query 
                testCase = new CsvTestCase();
                testCase.query = line.endsWith(";") ? line.substring(0, line.length() - 1) : line;
            }
            else {
                // read CSV header
                //            if (fragment.columnNames == null) {
                //                fragment.columnNames = line;
                //            }
                // read data
                if (line.startsWith(";")) {
                    testCase.expectedResults = data.toString();
                    // clean-up and emit
                    CsvTestCase result = testCase;
                    testCase = null;
                    data.setLength(0);
                    return result;
                }
                else {
                    data.append(line);
                    data.append("\r\n");
                }
            }

            return null;
        }
    }

    private static class CsvTestCase {
        String query;
        String expectedResults;
    }
}
