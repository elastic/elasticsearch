/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;

public class ShowIT extends CliIntegrationTestCase {
    public void testShowTables() throws IOException {
        index("test1", body -> body.field("test_field", "test_value"));
        index("test2", body -> body.field("test_field", "test_value"));
        command("SHOW TABLES");
        assertThat(in.readLine(), RegexMatcher.matches("\\s*index\\s*\\|\\s*type\\s*"));
        assertThat(in.readLine(), containsString("----------"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*test[12]\\s*\\|\\s*doc\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*test[12]\\s*\\|\\s*doc\\s*"));
        assertEquals("", in.readLine());
    }

    public void testShowFunctions() throws IOException {
        command("SHOW FUNCTIONS");
        assertThat(in.readLine(), RegexMatcher.matches("\\s*name\\s*\\|\\s*type\\s*"));
        assertThat(in.readLine(), containsString("----------"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*AVG\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*COUNT\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*MAX\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*MIN\\s*\\|\\s*AGGREGATE\\s*"));
        String line = in.readLine();
        Pattern aggregateFunction = Pattern.compile("\\s*[A-Z0-9_~]+\\s*\\|\\s*AGGREGATE\\s*");
        while (aggregateFunction.matcher(line).matches()) {
            line = in.readLine();
        }
        Pattern scalarFunction = Pattern.compile("\\s*[A-Z0-9_~]+\\s*\\|\\s*SCALAR\\s*");
        while (scalarFunction.matcher(line).matches()) {
            line = in.readLine();
        }
        assertEquals("", line);

        command("SHOW FUNCTIONS LIKE 'L%'");
        assertThat(in.readLine(), RegexMatcher.matches("\\s*name\\s*\\|\\s*type\\s*"));
        assertThat(in.readLine(), containsString("----------"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*LOG\\s*\\|\\s*SCALAR\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*LOG10\\s*\\|\\s*SCALAR\\s*"));
        assertEquals("", in.readLine());

        command("SHOW FUNCTIONS LIKE '%DAY%'");
        assertThat(in.readLine(), RegexMatcher.matches("\\s*name\\s*\\|\\s*type\\s*"));
        assertThat(in.readLine(), containsString("----------"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*DAY_OF_MONTH\\s*\\|\\s*SCALAR\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*DAY\\s*\\|\\s*SCALAR\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*DAY_OF_WEEK\\s*\\|\\s*SCALAR\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*DAY_OF_YEAR\\s*\\|\\s*SCALAR\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*HOUR_OF_DAY\\s*\\|\\s*SCALAR\\s*"));
        assertThat(in.readLine(), RegexMatcher.matches("\\s*MINUTE_OF_DAY\\s*\\|\\s*SCALAR\\s*"));
        assertEquals("", in.readLine());
    }
}
