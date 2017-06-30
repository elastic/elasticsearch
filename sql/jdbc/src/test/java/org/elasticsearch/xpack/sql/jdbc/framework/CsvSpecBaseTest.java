/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util.framework;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;

import static java.lang.String.format;

import static org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcAssert.assertResultSets;

public abstract class CsvSpecBaseTest extends SpecBaseTest {

    @Parameter(3)
    public CsvFragment fragment;

    protected static List<Object[]> readScriptSpec(String url) throws Exception {
        return SpecBaseTest.readScriptSpec(url, new CsvSpecParser());
    }

    @Test
    public void testQuery() throws Throwable {
        try (Connection csv = CsvInfraSuite.csvCon(fragment.asProps(), fragment.reader).get();
             Connection es = CsvInfraSuite.esCon().get()) {
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
            throw new RuntimeException(errorMessage(th), th);
        }
    }

    String errorMessage(Throwable th) {
        return format(Locale.ROOT, "test%s@%s:%d failed\n\"%s\"\n%s", testName, source.getFileName().toString(), lineNumber, fragment.query, th.getMessage());
    }
}

class CsvSpecParser implements SpecBaseTest.Parser {

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

class CsvFragment {
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