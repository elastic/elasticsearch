/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.CsvTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.specParser;

/**
 * Test class to check https://github.com/elastic/elasticsearch/issues/35176 fix
 */
public class JdbcCsvNestedDocsIT extends JdbcCsvSpecIT {

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> tests = new ArrayList<>();
        tests.addAll(readScriptSpec("/nested.csv-spec", specParser()));
        return tests;
    }
    
    public JdbcCsvNestedDocsIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }

    @Override
    protected ResultSet executeJdbcQuery(Connection con, String query) throws SQLException {
        Statement statement = con.createStatement();
        statement.setFetchSize(randomIntBetween(1, 5));
        return statement.executeQuery(query);
    }
}
