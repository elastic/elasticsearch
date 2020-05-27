/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import org.elasticsearch.test.hamcrest.RegexMatcher;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public abstract class SelectTestCase extends CliIntegrationTestCase {
    public void testSelect() throws IOException {
        index("test", body -> body.field("test_field", "test_value"));
        assertThat(command("SELECT * FROM test"), containsString("test_field"));
        assertThat(readLine(), containsString("----------"));
        assertThat(readLine(), containsString("test_value"));
        assertEquals("", readLine());
    }

    public void testMultiLineSelect() throws IOException {
        index("test", body -> body.field("test_field", "test_value"));
        assertThat(command("SELECT *\nFROM\ntest"), containsString("test_field"));
        assertThat(readLine(), containsString("----------"));
        assertThat(readLine(), containsString("test_value"));
        assertEquals("", readLine());
    }

    public void testSelectWithWhere() throws IOException {
        index("test", body -> body.field("test_field", "test_value1").field("i", 1));
        index("test", body -> body.field("test_field", "test_value2").field("i", 2));
        assertThat(command("SELECT * FROM test WHERE i = 2"), RegexMatcher.matches("\\s*i\\s*\\|\\s*test_field\\s*"));
        assertThat(readLine(), containsString("----------"));
        assertThat(readLine(), RegexMatcher.matches("\\s*2\\s*\\|\\s*test_value2\\s*"));
        assertEquals("", readLine());
    }
}
