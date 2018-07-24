/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.jdbc;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.relique.io.TableReader;
import org.relique.jdbc.csv.CsvConnection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.Assert.assertThat;

/**
 * Utility functions for CSV testing
 */
public final class CsvTestUtils {

    private CsvTestUtils() {

    }

    /**
     * Executes a query on provided CSV connection.
     * <p>
     * The supplied table name is only used for the test identification.
     */
    public static ResultSet executeCsvQuery(Connection csv, String csvTableName) throws SQLException {
        ResultSet expected = csv.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
                .executeQuery("SELECT * FROM " + csvTableName);
        // trigger data loading for type inference
        expected.beforeFirst();
        return expected;
    }

    /**
     * Wraps CSV in the expectedResults into CSV Connection.
     *
     * Use {@link #executeCsvQuery} to obtain ResultSet from this connection
     */
    public static Connection csvConnection(String expectedResults) throws IOException, SQLException {
        Properties csvProperties = new Properties();
        csvProperties.setProperty("charset", "UTF-8");
        csvProperties.setProperty("separator", "|");
        csvProperties.setProperty("trimValues", "true");
        Tuple<String, String> resultsAndTypes = extractColumnTypesAndStripCli(expectedResults);
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
        return new CsvConnection(tableReader, csvProperties, "") {
        };
    }

    private static Tuple<String, String> extractColumnTypesAndStripCli(String expectedResults) throws IOException {
        try (StringReader reader = new StringReader(expectedResults);
             BufferedReader bufferedReader = new BufferedReader(reader);
             StringWriter writer = new StringWriter();
             BufferedWriter bufferedWriter = new BufferedWriter(writer)) {

            String header = bufferedReader.readLine();
            Tuple<String, String> headerAndTypes;

            if (header.contains(":")) {
                headerAndTypes = extractColumnTypesFromHeader(header);
            } else {
                // No type information in headers, no need to parse columns - trigger auto-detection
                headerAndTypes = new Tuple<>(header, "");
            }
            bufferedWriter.write(headerAndTypes.v1());
            bufferedWriter.newLine();

            /* Read the next line. It might be a separator designed to look like the cli.
             * If it is, then throw it out. If it isn't then keep it.
             */
            String maybeSeparator = bufferedReader.readLine();
            if (maybeSeparator != null && false == maybeSeparator.startsWith("----")) {
                bufferedWriter.write(maybeSeparator);
                bufferedWriter.newLine();
            }

            bufferedWriter.flush();
            // Copy the rest of test
            Streams.copy(bufferedReader, bufferedWriter);
            return new Tuple<>(writer.toString(), headerAndTypes.v2());
        }
    }

    private static Tuple<String, String> extractColumnTypesFromHeader(String header) {
        String[] columnTypes = Strings.delimitedListToStringArray(header, "|", " \t");
        StringBuilder types = new StringBuilder();
        StringBuilder columns = new StringBuilder();
        for (String column : columnTypes) {
            String[] nameType = Strings.delimitedListToStringArray(column, ":");
            assertThat("If at least one column has a type associated with it, all columns should have types", nameType, arrayWithSize(2));
            if (types.length() > 0) {
                types.append(",");
                columns.append("|");
            }
            columns.append(nameType[0]);
            types.append(resolveColumnType(nameType[1]));
        }
        return new Tuple<>(columns.toString(), types.toString());
    }

    private static String resolveColumnType(String type) {
        switch (type.toLowerCase(Locale.ROOT)) {
            case "s":
                return "string";
            case "b":
                return "boolean";
            case "i":
                return "integer";
            case "l":
                return "long";
            case "f":
                return "float";
            case "d":
                return "double";
            case "ts":
                return "timestamp";
            default:
                return type;
        }
    }

    /**
     * Returns an instance of a parser for csv-spec tests.
     */
    public static CsvSpecParser specParser() {
        return new CsvSpecParser();
    }

    private static class CsvSpecParser implements SpecBaseIntegrationTestCase.Parser {
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

    public static class CsvTestCase {
        public String query;
        public String expectedResults;
    }
}