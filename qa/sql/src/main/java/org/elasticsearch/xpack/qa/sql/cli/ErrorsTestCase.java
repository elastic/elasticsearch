/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.cli;

/**
 * Tests for error messages.
 */
public abstract class ErrorsTestCase extends CliIntegrationTestCase {
    public void testSelectFromMissingTable() throws Exception {
        assertEquals("[1;31mBad request [[22;3;33mFound 1 problem(s)", command("SELECT * FROM test"));
        assertEquals("line 1:15: Unknown index [test][1;23;31m][0m", readLine());
    }
}
