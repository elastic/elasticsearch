/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.csv;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.xpack.sql.jdbc.framework.CsvSpecTableReader;
import org.elasticsearch.xpack.sql.jdbc.framework.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.util.CollectionUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.relique.jdbc.csv.CsvDriver;

import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.jdbc.framework.JdbcAssert.assertResultSets;

public class CsvSpecIntegrationTest extends SpecBaseIntegrationTestCase {

    private static CsvDriver DRIVER = new CsvDriver();
    public static final Map<Connection, Reader> CSV_READERS = new LinkedHashMap<>();

    private final CsvFragment fragment;

    @AfterClass
    public static void cleanup() throws Exception {
        CSV_READERS.clear();
    }

    public static CheckedSupplier<Connection, SQLException> csvCon(Properties props, Reader reader) {
        return new CheckedSupplier<Connection, SQLException>() {
            @Override
            public Connection get() throws SQLException {
                Connection con = DRIVER.connect("jdbc:relique:csv:class:" + CsvSpecTableReader.class.getName(), props);
                CSV_READERS.put(con, reader);
                return con;
            }
        };
    }

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTNG)
    public static List<Object[]> readScriptSpec() throws Exception {
        CsvSpecParser parser = new CsvSpecParser();
        return CollectionUtils.combine(
                readScriptSpec("/command.csv-spec", parser),
                readScriptSpec("/fulltext.csv-spec", parser));
    }


    public CsvSpecIntegrationTest(String groupName, String testName, Integer lineNumber, Path source, CsvFragment fragment) {
        super(groupName, testName, lineNumber, source);
        this.fragment = fragment;
    }

    @Test
    public void testQuery() throws Throwable {
        // hook CSV reader, which picks the current test context
        try (Connection csv = csvCon(fragment.asProps(), fragment.reader).get();
             Connection es = esCon()) {
            ResultSet expected, actual;
            try {
                // pass the testName as table for debugging purposes (in case the underlying reader is missing)
                expected = csv.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery("SELECT * FROM " + testName);
                // trigger data loading for type inference
                expected.beforeFirst();
                actual = es.createStatement().executeQuery(fragment.query);
                assertResultSets(expected, actual);
            } catch (AssertionError ae) {
                throw reworkException(new AssertionError(errorMessage(ae), ae.getCause()));
            }
        } catch (Throwable th) {
            throw reworkException(th);
        }
    }

    String errorMessage(Throwable th) {
        return format(Locale.ROOT, "test%s@%s:%d failed\n\"%s\"\n%s", testName, source.getFileName().toString(), lineNumber, fragment.query, th.getMessage());
    }

    private static class CsvSpecParser implements Parser {

        private final StringBuilder data = new StringBuilder();
        private CsvFragment fragment;

        @Override
        public Object parse(String line) {
            // beginning of the section
            if (fragment == null) {
                // pick up the query 
                fragment = new CsvFragment();
                fragment.query = line.endsWith(";") ? line.substring(0, line.length() - 1) : line;
            }
            else {
                // read CSV header
                //            if (fragment.columnNames == null) {
                //                fragment.columnNames = line;
                //            }
                // read data
                if (line.startsWith(";")) {
                    CsvFragment f = fragment;
                    f.reader = new StringReader(data.toString());
                    // clean-up
                    fragment = null;
                    data.setLength(0);
                    return f;
                }
                else {
                    data.append(line);
                    data.append("\r\n");
                }
            }

            return null;
        }
    }

    private static class CsvFragment {
        String query;
        String columnNames;
        List<String> columnTypes;
        Reader reader;

        private static final Properties DEFAULT = new Properties();

        static {
            DEFAULT.setProperty("charset", "UTF-8");
            // trigger auto-detection
            DEFAULT.setProperty("columnTypes", "");
            DEFAULT.setProperty("separator", "|");
            DEFAULT.setProperty("trimValues", "true");
        }

        Properties asProps() {
            //        p.setProperty("suppressHeaders", "true");
            //        p.setProperty("headerline", columnNames);
            return DEFAULT;
        }
    }
}
