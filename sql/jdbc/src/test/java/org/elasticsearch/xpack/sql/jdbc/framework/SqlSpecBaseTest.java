/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util.framework;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Locale;

import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;

import static java.lang.String.format;

import static org.elasticsearch.xpack.sql.jdbc.integration.util.JdbcAssert.assertResultSets;

public abstract class SqlSpecBaseTest extends SpecBaseTest {

    @Parameter(3)
    public String query;

    protected static List<Object[]> readScriptSpec(String url) throws Exception {
        return SpecBaseTest.readScriptSpec(url, new SqlSpecParser());
    }

    @Test
    public void testQuery() throws Throwable {
        // H2 resultset
        try (Connection h2 = SqlInfraSuite.h2Con().get();
             Connection es = SqlInfraSuite.esCon().get()) {
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

    String errorMessage(Throwable th) {
        return format(Locale.ROOT, "test%s@%s:%d failed\n\"%s\"\n%s", testName, source.getFileName().toString(), lineNumber, query, th.getMessage());
    }
}

class SqlSpecParser implements SpecBaseTest.Parser {

    @Override
    public Object parse(String line) {
        return line.endsWith(";") ? line.substring(0, line.length() - 1) : line;
    }
}