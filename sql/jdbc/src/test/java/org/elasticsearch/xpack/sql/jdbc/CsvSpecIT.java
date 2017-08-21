/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.xpack.sql.jdbc.framework.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.util.CollectionUtils;
import org.relique.io.TableReader;
import org.relique.jdbc.csv.CsvConnection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.hamcrest.Matchers.arrayWithSize;

/**
 * Tests comparing sql queries executed against our jdbc client
 * with hard coded result sets.
 */
public class CsvSpecIT extends SpecBaseIntegrationTestCase {
    private final CsvTestCase testCase;

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        Parser parser = specParser();
        return CollectionUtils.combine(
                readScriptSpec("/command.csv-spec", parser),
                readScriptSpec("/fulltext.csv-spec", parser),
                readScriptSpec("/agg.csv-spec", parser),
                readScriptSpec("/columns.csv-spec", parser)
                );
    }

    public CsvSpecIT(String groupName, String testName, Integer lineNumber, Path source, CsvTestCase testCase) {
        super(groupName, testName, lineNumber, source);
        this.testCase = testCase;
    }

    @Override
    protected final void doTest() throws Throwable {
        assertMatchesCsv(testCase.query, testName, testCase.expectedResults);
    }

    private void assertMatchesCsv(String query, String csvTableName, String expectedResults) throws SQLException, IOException {
        Properties csvProperties = new Properties();
        csvProperties.setProperty("charset", "UTF-8");
        csvProperties.setProperty("separator", "|");
        csvProperties.setProperty("trimValues", "true");
        Tuple<String,String> resultsAndTypes = extractColumnTypes(expectedResults);
        csvProperties.setProperty("columnTypes", resultsAndTypes.v2());
        Reader reader = new StringReader(resultsAndTypes.v1());
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
        try (Connection csv = new CsvConnection(tableReader, csvProperties, "") {};
             Connection es = esJdbc()) {
            // pass the testName as table for debugging purposes (in case the underlying reader is missing)
            ResultSet expected = csv.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
                    .executeQuery("SELECT * FROM " + csvTableName);
            // trigger data loading for type inference
            expected.beforeFirst();
            ResultSet elasticResults = executeJdbcQuery(es, query);
            assertResults(expected, elasticResults);
        }
    }

    private Tuple<String,String> extractColumnTypes(String expectedResults) throws IOException {
        try (StringReader reader = new StringReader(expectedResults)){
            try (BufferedReader bufferedReader = new BufferedReader(reader)){
                String header = bufferedReader.readLine();
                if (!header.contains(":")) {
                    // No type information in headers, no need to parse columns - trigger auto-detection
                    return new Tuple<>(expectedResults,"");
                }
                try (StringWriter writer = new StringWriter()) {
                    try (BufferedWriter bufferedWriter = new BufferedWriter(writer)){
                        Tuple<String, String> headerAndColumns = extractColumnTypesFromHeader(header);
                        bufferedWriter.write(headerAndColumns.v1());
                        bufferedWriter.newLine();
                        bufferedWriter.flush();
                        // Copy the rest of test
                        Streams.copy(bufferedReader, bufferedWriter);
                        return new Tuple<>(writer.toString(), headerAndColumns.v2());
                    }
                }
            }
        }
    }

    private Tuple<String,String> extractColumnTypesFromHeader(String header) {
        String[] columnTypes = Strings.delimitedListToStringArray(header, "|", " \t");
        StringBuilder types = new StringBuilder();
        StringBuilder columns = new StringBuilder();
        for(String column : columnTypes) {
            String[] nameType = Strings.delimitedListToStringArray(column, ":");
            assertThat("If at least one column has a type associated with it, all columns should have types",  nameType, arrayWithSize(2));
            if(types.length() > 0) {
                types.append(",");
                columns.append("|");
            }
            columns.append(nameType[0]);
            types.append(resolveColumnType(nameType[1]));
        }
        return new Tuple<>(columns.toString(), types.toString());
    }
    
    private String resolveColumnType(String type) {
        switch (type.toLowerCase(Locale.ROOT)) {
            case "s": return "string";
            case "b": return "boolean";
            case "i": return "integer";
            case "l": return "long";
            case "f": return "float";
            case "d": return "double";
            default: return type;
        }
    }

    static CsvSpecParser specParser() {
        return new CsvSpecParser();
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