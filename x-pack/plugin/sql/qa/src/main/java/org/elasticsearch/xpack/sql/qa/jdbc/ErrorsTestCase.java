/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;

import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for exceptions and their messages.
 */
public class ErrorsTestCase extends JdbcIntegrationTestCase implements org.elasticsearch.xpack.sql.qa.ErrorsTestCase {
    @Override
    public void testSelectInvalidSql() throws Exception {
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * FRO").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:8: Cannot determine columns for [*]", e.getMessage());
        }
    }

    @Override
    public void testSelectFromMissingIndex() throws SQLException {
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * FROM test").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:15: Unknown index [test]", e.getMessage());
        }
    }

    @Override
    public void testSelectFromIndexWithoutTypes() throws Exception {
        // Create an index without any types
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("{}");
        client().performRequest(request);

        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT * FROM test").executeQuery());
            // see https://github.com/elastic/elasticsearch/issues/34719
            //assertEquals("Found 1 problem(s)\nline 1:15: [test] doesn't have any types so it is incompatible with sql", e.getMessage());
            assertEquals("Found 1 problem(s)\nline 1:15: Unknown index [test]", e.getMessage());
        }
    }

    @Override
    public void testSelectMissingField() throws Exception {
        index("test", body -> body.field("test", "test"));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT missing FROM test").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:8: Unknown column [missing]", e.getMessage());
        }
    }

    @Override
    public void testSelectMissingFunction() throws Exception {
        index("test", body -> body.field("foo", 1));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () -> c.prepareStatement("SELECT missing(foo) FROM test").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:8: Unknown function [missing]", e.getMessage());
        }
    }

    @Override
    public void testSelectProjectScoreInAggContext() throws Exception {
        index("test", body -> body.field("foo", 1));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () ->
                c.prepareStatement("SELECT foo, SCORE(), COUNT(*) FROM test GROUP BY foo").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:13: Cannot use non-grouped column [SCORE()], expected [foo]", e.getMessage());
        }
    }

    @Override
    public void testSelectOrderByScoreInAggContext() throws Exception {
        index("test", body -> body.field("foo", 1));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () ->
                c.prepareStatement("SELECT foo, COUNT(*) FROM test GROUP BY foo ORDER BY SCORE()").executeQuery());
            assertEquals(
                    "Found 1 problem(s)\nline 1:54: Cannot order by non-grouped column [SCORE()], expected [foo] or an aggregate function",
                    e.getMessage());
        }
    }

    @Override
    public void testSelectGroupByScore() throws Exception {
        index("test", body -> body.field("foo", 1));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () ->
                c.prepareStatement("SELECT COUNT(*) FROM test GROUP BY SCORE()").executeQuery());
            assertEquals("Found 1 problem(s)\nline 1:36: Cannot use [SCORE()] for grouping", e.getMessage());
        }
    }

    @Override
    public void testSelectScoreSubField() throws Exception {
        index("test", body -> body.field("foo", 1));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () ->
                c.prepareStatement("SELECT SCORE().bar FROM test").executeQuery());
            assertThat(e.getMessage(), startsWith("line 1:15: extraneous input '.' expecting {<EOF>, ','"));
        }
    }

    @Override
    public void testSelectScoreInScalar() throws Exception {
        index("test", body -> body.field("foo", 1));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () ->
                c.prepareStatement("SELECT SIN(SCORE()) FROM test").executeQuery());
            assertThat(e.getMessage(), startsWith("Found 1 problem(s)\nline 1:12: [SCORE()] cannot be an argument to a function"));
        }
    }

    @Override
    public void testHardLimitForSortOnAggregate() throws Exception {
        index("test", body -> body.field("a", 1).field("b", 2));
        try (Connection c = esJdbc()) {
            SQLException e = expectThrows(SQLException.class, () ->
                c.prepareStatement("SELECT max(a) max FROM test GROUP BY b ORDER BY max LIMIT 12000").executeQuery());
            assertEquals("The maximum LIMIT for aggregate sorting is [10000], received [12000]", e.getMessage());
        }
    }
}
