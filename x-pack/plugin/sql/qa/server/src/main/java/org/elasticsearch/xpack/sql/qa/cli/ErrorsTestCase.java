/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import org.elasticsearch.client.Request;

import java.io.IOException;

import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for error messages.
 */
public abstract class ErrorsTestCase extends CliIntegrationTestCase implements org.elasticsearch.xpack.sql.qa.ErrorsTestCase {
    /**
     * Starting sequence commons to lots of errors.
     */
    public static final String START = "[?1l>[?1000l[?2004l[31;1m";
    /**
     * Ending sequence common to lots of errors.
     */
    public static final String END = "[23;31;1m][0m";

    @Override
    public void testSelectInvalidSql() throws Exception {
        assertFoundOneProblem(command("SELECT * FRO"));
        assertEquals("line 1:8: Cannot determine columns for [*]" + END, readLine());
    }

    @Override
    public void testSelectFromMissingIndex() throws IOException {
        assertFoundOneProblem(command("SELECT * FROM test"));
        assertEquals("line 1:15: Unknown index [test]" + END, readLine());
    }

    @Override
    public void testSelectColumnFromMissingIndex() throws Exception {
        assertFoundOneProblem(command("SELECT abc FROM test"));
        assertEquals("line 1:17: Unknown index [test]" + END, readLine());
    }

    @Override
    public void testSelectColumnFromEmptyIndex() throws Exception {
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("{}");
        client().performRequest(request);

        assertFoundOneProblem(command("SELECT abc FROM test"));
        assertEquals("line 1:8: Unknown column [abc]" + END, readLine());
    }

    @Override
    public void testSelectMissingField() throws IOException {
        index("test", body -> body.field("test", "test"));
        assertFoundOneProblem(command("SELECT missing FROM test"));
        assertEquals("line 1:8: Unknown column [missing]" + END, readLine());
    }

    @Override
    public void testSelectMissingFunction() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertFoundOneProblem(command("SELECT missing(foo) FROM test"));
        assertEquals("line 1:8: Unknown function [missing]" + END, readLine());
    }

    @Override
    public void testSelectProjectScoreInAggContext() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertFoundOneProblem(command("SELECT foo, SCORE(), COUNT(*) FROM test GROUP BY foo"));
        assertEquals("line 1:13: Cannot use non-grouped column [SCORE()], expected [foo]" + END, readLine());
    }

    @Override
    public void testSelectOrderByScoreInAggContext() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertFoundOneProblem(command("SELECT foo, COUNT(*) FROM test GROUP BY foo ORDER BY SCORE()"));
        assertEquals("line 1:54: Cannot order by non-grouped column [SCORE()], expected [foo] or an aggregate function" + END, readLine());
    }

    @Override
    public void testSelectGroupByScore() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertFoundOneProblem(command("SELECT COUNT(*) FROM test GROUP BY SCORE()"));
        assertEquals("line 1:36: Cannot use [SCORE()] for grouping" + END, readLine());
    }

    @Override
    public void testSelectScoreSubField() throws Exception {
        index("test", body -> body.field("foo", 1));
        assertThat(
            command("SELECT SCORE().bar FROM test"),
            startsWith(START + "Bad request [[3;33;22mline 1:15: mismatched input '.' expecting {<EOF>, ")
        );
    }

    @Override
    public void testHardLimitForSortOnAggregate() throws Exception {
        index("test", body -> body.field("a", 1).field("b", 2));
        String commandResult = command("SELECT max(a) max FROM test GROUP BY b ORDER BY max LIMIT 120000");
        assertEquals(
            START + "Bad request [[3;33;22mThe maximum LIMIT for aggregate sorting is [65536], received [120000]" + END,
            commandResult
        );
    }

    public static void assertFoundOneProblem(String commandResult) {
        assertEquals(START + "Bad request [[3;33;22mFound 1 problem", commandResult);
    }
}
