/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.xpack.sql.qa.jdbc.CsvSpecTestCase;
import org.elasticsearch.xpack.sql.qa.jdbc.CsvTestUtils.CsvTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcCsvSpecIT extends CsvSpecTestCase {
    public JdbcCsvSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase) {
        super(fileName, groupName, testName, lineNumber, testCase);
    }

    @Override
    protected ResultSet executeJdbcQuery(Connection con, String query) throws SQLException {
        // using a smaller fetchSize for nested documents' tests to uncover bugs
        // similar with https://github.com/elastic/elasticsearch/issues/35176 quicker
        if (fileName.startsWith("nested")) {
            Statement statement = con.createStatement();
            statement.setFetchSize(randomBoolean() ? fetchSize() : randomIntBetween(1, 5));
            return statement.executeQuery(query);
        }
        return super.executeJdbcQuery(con, query);
    }
}
