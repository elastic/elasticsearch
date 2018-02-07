/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.cli;

import java.io.IOException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import static java.util.Collections.emptyMap;

import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for error messages.
 */
public abstract class ErrorsTestCase extends CliIntegrationTestCase implements org.elasticsearch.xpack.qa.sql.ErrorsTestCase {
    @Override
    public void testSelectInvalidSql() throws Exception {
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)", command("SELECT * FRO"));
        assertEquals("line 1:8: Cannot determine columns for *[1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectFromMissingIndex() throws IOException {
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)", command("SELECT * FROM test"));
        assertEquals("line 1:15: Unknown index [test][1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectFromIndexWithoutTypes() throws Exception {
        // Create an index without any types
        client().performRequest("PUT", "/test", emptyMap(), new StringEntity("{}", ContentType.APPLICATION_JSON));

        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)", command("SELECT * FROM test"));
        assertEquals("line 1:15: [test] doesn't have any types so it is incompatible with sql[1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectMissingField() throws IOException {
        index("test", body -> body.field("test", "test"));
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)", command("SELECT missing FROM test"));
        assertEquals("line 1:8: Unknown column [missing][1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectMissingFunction() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)", command("SELECT missing(foo) FROM test"));
        assertEquals("line 1:8: Unknown function [missing][1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectProjectScoreInAggContext() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)", command("SELECT foo, SCORE(), COUNT(*) FROM test GROUP BY foo"));
        assertEquals("line 1:13: Cannot use non-grouped column [SCORE()], expected [foo][1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectOrderByScoreInAggContext() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)",
            command("SELECT foo, COUNT(*) FROM test GROUP BY foo ORDER BY SCORE()"));
        assertEquals("line 1:54: Cannot order by non-grouped column [SCORE()], expected [foo][1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectGroupByScore() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)",
            command("SELECT COUNT(*) FROM test GROUP BY SCORE()"));
        assertEquals("line 1:36: Cannot use [SCORE()] for grouping[1;23;31m][0m", readLine());
    }

    @Override
    public void testSelectScoreSubField() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertThat(command("SELECT SCORE().bar FROM test"),
            startsWith("[1;31mBad request [[22;3;33mline 1:15: extraneous input '.' expecting {<EOF>, ',',"));
    }

    @Override
    public void testSelectScoreInScalar() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)",
            command("SELECT SIN(SCORE()) FROM test"));
        assertEquals("line 1:12: [SCORE()] cannot be an argument to a function[1;23;31m][0m", readLine());
    }
}
