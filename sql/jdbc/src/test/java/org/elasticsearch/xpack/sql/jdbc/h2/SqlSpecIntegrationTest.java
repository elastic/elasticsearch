/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.h2;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.jdbc.framework.LocalH2;
import org.elasticsearch.xpack.sql.jdbc.framework.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.jdbc.framework.SpecBaseIntegrationTestCase.Parser;
import org.elasticsearch.xpack.sql.util.CollectionUtils;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.jdbc.framework.JdbcAssert.assertResultSets;

public class SqlSpecIntegrationTest extends SpecBaseIntegrationTestCase {

    private String query;

    @ClassRule
    public static LocalH2 H2 = new LocalH2();

    @ParametersFactory(shuffle = false, argumentFormatting = "name=%1s")
    public static List<Object[]> readScriptSpec() throws Exception {
        SqlSpecParser parser = new SqlSpecParser();
        return CollectionUtils.combine(
                readScriptSpec("/select.sql-spec", parser),
                readScriptSpec("/filter.sql-spec", parser),
                readScriptSpec("/datetime.sql-spec", parser),
                readScriptSpec("/math.sql-spec", parser),
                readScriptSpec("/agg.sql-spec", parser));
    }

    private static class SqlSpecParser implements Parser {
        @Override
        public Object parse(String line) {
            return line.endsWith(";") ? line.substring(0, line.length() - 1) : line;
        }
    }

    public Connection h2Con() throws SQLException {
        return H2.get();
    }
    
    public SqlSpecIntegrationTest(String groupName, @Name("testName") String testName, Integer lineNumber, Path source, String query) {
        super(groupName, testName, lineNumber, source);
        this.query = query;
    }

    @Test
    public void testQuery() throws Throwable {
        // H2 resultset
        try (Connection h2 = h2Con(); 
             Connection es = esCon()) {
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