/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.test.hamcrest.RegexMatcher;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class SelectIT extends CliIntegrationTestCase {
    public void testSelect() throws IOException {
        index("test", body -> body.field("test_field", "test_value"));
        command("SELECT * FROM test");
        assertThat(in.readLine(), containsString("test_field"));
        assertThat(in.readLine(), containsString("----------"));
        assertThat(in.readLine(), containsString("test_value"));
        assertEquals("", in.readLine());
    }

    public void testSelectWithWhere() throws IOException {
        index("test", body -> body.field("test_field", "test_value1").field("i", 1));
        index("test", body -> body.field("test_field", "test_value2").field("i", 2));
        command("SELECT * FROM test WHERE i = 2");
        assertThat(in.readLine(), RegexMatcher.matches("\\s*i\\s*\\|\\s*test_field\\s*"));
        assertThat(in.readLine(), containsString("----------"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*2\\s*\\|\\s*test_value2\\s*"));
        assertEquals("", in.readLine());
    }
}
