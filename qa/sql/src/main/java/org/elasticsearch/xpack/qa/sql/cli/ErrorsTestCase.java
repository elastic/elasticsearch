/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.cli;

import java.io.IOException;

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
}
