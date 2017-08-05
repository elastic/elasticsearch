/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.jdbc.framework.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.util.CollectionUtils;
import org.relique.io.TableReader;
import org.relique.jdbc.csv.CsvConnection;

import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.xpack.sql.jdbc.framework.JdbcAssert.assertResultSets;

/**
 * Tests comparing sql queries executed against our jdbc client
 * with hard coded result sets.
 */
public class CsvSpecIT extends SpecBaseIntegrationTestCase {
    /**
     * Properties used when settings up a CSV-based jdbc connection.
     */
    private static final Properties CSV_PROPERTIES = new Properties();
    static {
        CSV_PROPERTIES.setProperty("charset", "UTF-8");
        // trigger auto-detection
        CSV_PROPERTIES.setProperty("columnTypes", "");
        CSV_PROPERTIES.setProperty("separator", "|");
        CSV_PROPERTIES.setProperty("trimValues", "true");
    }

    private final CsvTestCase testCase;

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        CsvSpecParser parser = new CsvSpecParser();
        return CollectionUtils.combine(
                readScriptSpec("/command.csv-spec", parser),
                readScriptSpec("/fulltext.csv-spec", parser),
                readScriptSpec("/agg.csv-spec", parser)
                );
    }

    public CsvSpecIT(String groupName, String testName, Integer lineNumber, Path source, CsvTestCase testCase) {
        super(groupName, testName, lineNumber, source);
        this.testCase = testCase;
    }

    public void test() throws Throwable {
        try {
            assertMatchesCsv(testCase.query, testName, testCase.expectedResults);            
        } catch (AssertionError ae) {
            throw reworkException(ae);
        }
    }

    private void assertMatchesCsv(String query, String csvTableName, String expectedResults) throws SQLException {
        Reader reader = new StringReader(expectedResults);
        TableReader tableReader = new TableReader() {
            @Override
            public Reader getReader(Statement statement, String tableName) throws SQLException {
                return reader;
            }

            @Override
            public List<String> getTableNames(Connection connection) throws SQLException {
                throw new UnsupportedOperationException();
            }
        };
        try (Connection csv = new CsvConnection(tableReader, CSV_PROPERTIES, "") {};
             Connection es = esJdbc()) {
            // pass the testName as table for debugging purposes (in case the underlying reader is missing)
            ResultSet expected = csv.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
                    .executeQuery("SELECT * FROM " + csvTableName);
            // trigger data loading for type inference
            expected.beforeFirst();
            ResultSet actual = executeJdbcQuery(es, query);
            assertResultSets(expected, actual);
        }
    }

    private ResultSet executeJdbcQuery(Connection con, String query) throws SQLException {
        Statement statement = con.createStatement();
        //statement.setFetchSize(randomInt(10));
        // NOCOMMIT: hook up pagination
        statement.setFetchSize(1000);
        return statement.executeQuery(query);
    }

    protected static class CsvSpecParser implements Parser {
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

    protected static class CsvTestCase {
        String query;
        String expectedResults;
    }
}