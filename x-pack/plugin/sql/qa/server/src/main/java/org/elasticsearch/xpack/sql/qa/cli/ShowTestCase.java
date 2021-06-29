/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import org.elasticsearch.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;

public abstract class ShowTestCase extends CliIntegrationTestCase {

    private static final String HEADER_SEPARATOR = "----------";

    public void testShowTables() throws IOException {
        index("test1", body -> body.field("test_field", "test_value"));
        index("test2", body -> body.field("test_field", "test_value"));
        assertThat(command("SHOW TABLES"), RegexMatcher.matches("\\s*name\\s*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), RegexMatcher.matches("\\s*test[12]\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*test[12]\\s*"));
        assertEquals("", readLine());
    }

    public void testShowFunctions() throws IOException {
        assertThat(command("SHOW FUNCTIONS"), RegexMatcher.matches("\\s*name\\s*\\|\\s*type\\s*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), RegexMatcher.matches("\\s*AVG\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*COUNT\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*FIRST\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*FIRST_VALUE\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LAST\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LAST_VALUE\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*MAX\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*MIN\\s*\\|\\s*AGGREGATE\\s*"));
        String line = readLine();
        Pattern aggregateFunction = Pattern.compile("\\s*[A-Z0-9_~]+\\s*\\|\\s*AGGREGATE\\s*");
        while (aggregateFunction.matcher(line).matches()) {
            line = readLine();
        }
        Pattern groupingFunction = Pattern.compile("\\s*[A-Z0-9_~]+\\s*\\|\\s*GROUPING\\s*");
        while (groupingFunction.matcher(line).matches()) {
            line = readLine();
        }
        Pattern conditionalFunction = Pattern.compile("\\s*[A-Z0-9_~]+\\s*\\|\\s*CONDITIONAL\\s*");
        while (conditionalFunction.matcher(line).matches()) {
            line = readLine();
        }
        Pattern scalarFunction = Pattern.compile("\\s*[A-Z0-9_~]+\\s*\\|\\s*SCALAR\\s*");
        while (scalarFunction.matcher(line).matches()) {
            line = readLine();
        }

        assertThat(line, RegexMatcher.matches("\\s*SCORE\\s*\\|\\s*SCORE\\s*"));
        assertEquals("", readLine());
    }

    public void testShowFunctionsLikePrefix() throws IOException {
        assertThat(command("SHOW FUNCTIONS LIKE 'L%'"), RegexMatcher.matches("\\s*name\\s*\\|\\s*type\\s*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), RegexMatcher.matches("\\s*LAST\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LAST_VALUE\\s*\\|\\s*AGGREGATE\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LEAST\\s*\\|\\s*CONDITIONAL\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LOG\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LOG10\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LCASE\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LEFT\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LENGTH\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LOCATE\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*LTRIM\\s*\\|\\s*SCALAR\\s*"));
        assertEquals("", readLine());
    }

    public void testShowFunctionsLikeInfix() throws IOException {
        assertThat(command("SHOW FUNCTIONS LIKE '%DAY%'"), RegexMatcher.matches("\\s*name\\s*\\|\\s*type\\s*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAY\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAYNAME\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAYOFMONTH\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAYOFWEEK\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAYOFYEAR\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAY_NAME\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAY_OF_MONTH\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAY_OF_WEEK\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*DAY_OF_YEAR\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*HOUR_OF_DAY\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*ISODAYOFWEEK\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*ISO_DAY_OF_WEEK\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*MINUTE_OF_DAY\\s*\\|\\s*SCALAR\\s*"));
        assertThat(readLine(), RegexMatcher.matches("\\s*TODAY\\s*\\|\\s*SCALAR\\s*"));
        assertEquals("", readLine());
    }
}
