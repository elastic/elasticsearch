/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.jdbc.framework.LocalH2;
import org.elasticsearch.xpack.sql.jdbc.framework.SpecBaseIntegrationTestCase;
import org.elasticsearch.xpack.sql.util.CollectionUtils;
import org.junit.Before;
import org.junit.ClassRule;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/**
 * Tests comparing sql queries executed against our jdbc client
 * with those executed against H2's jdbc client.
 */
public class SqlSpecIT extends SpecBaseIntegrationTestCase {
    private String query;

    @ClassRule
    public static LocalH2 H2 = new LocalH2();

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {

        // example for enabling logging
        //JdbcTestUtils.sqlLogging();

        Parser parser = specParser();
        return CollectionUtils.combine(
                readScriptSpec("/select.sql-spec", parser),
                readScriptSpec("/filter.sql-spec", parser),
                readScriptSpec("/datetime.sql-spec", parser),
                readScriptSpec("/math.sql-spec", parser),
                readScriptSpec("/agg.sql-spec", parser));
    }

    // NOCOMMIT: add tests for nested docs when interplug communication is enabled
    //    "DESCRIBE emp.emp",
    //    "SELECT dep FROM emp.emp",
    //    "SELECT dep.dept_name, first_name, last_name FROM emp.emp WHERE emp_no = 10020",
    //    "SELECT first_name f, last_name l, dep.from_date FROM emp.emp WHERE dep.dept_name = 'Production' ORDER BY dep.from_date",
    //    "SELECT first_name f, last_name l, YEAR(dep.from_date) start "
    //    + "FROM emp.emp WHERE dep.dept_name = 'Production' AND tenure > 30 ORDER BY start"

    private static class SqlSpecParser implements Parser {
        @Override
        public Object parse(String line) {
            return line.endsWith(";") ? line.substring(0, line.length() - 1) : line;
        }
    }

    static SqlSpecParser specParser() {
        return new SqlSpecParser();
    }

    public SqlSpecIT(String groupName, String testName, Integer lineNumber, Path source, String query) {
        super(groupName, testName, lineNumber, source);
        this.query = query;
    }

    @Before
    public void testDateTime() {
        assumeFalse("Date time tests have time zone problems", "datetime".equals(groupName));
    }

    @Override
    protected final void doTest() throws Throwable {
        try (Connection h2 = H2.get(); 
             Connection es = esJdbc()) {

            ResultSet expected, elasticResults;
            expected = executeJdbcQuery(h2, query);
            elasticResults = executeJdbcQuery(es, query);

            assertResults(expected, elasticResults);
        }
    }
}