/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;

public abstract class ShowTestCase extends CliIntegrationTestCase {

    private static final String HEADER_SEPARATOR = "----------";

    public void testShowTables() throws IOException {
        index("test1", body -> body.field("test_field", "test_value"));
        index("test2", body -> body.field("test_field", "test_value"));
        assertThat(command("SHOW TABLES"), matchesRegex(".*\\s*name\\s*.*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), matchesRegex(".*\\s*test[12]\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*test[12]\\s*.*"));
        assertThat(readLine(), is(emptyString()));
    }

    public void testShowFunctions() throws IOException {
        assertThat(command("SHOW FUNCTIONS"), matchesRegex(".*\\s*name\\s*\\|\\s*type\\s*.*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), matchesRegex(".*\\s*AVG\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*COUNT\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*FIRST\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*FIRST_VALUE\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LAST\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LAST_VALUE\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*MAX\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*MIN\\s*\\|\\s*AGGREGATE\\s*.*"));
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

        assertThat(line, matchesRegex(".*\\s*SCORE\\s*\\|\\s*SCORE\\s*.*"));
        assertThat(readLine(), is(emptyString()));
    }

    public void testShowFunctionsLikePrefix() throws IOException {
        assertThat(command("SHOW FUNCTIONS LIKE 'L%'"), matchesRegex(".*\\s*name\\s*\\|\\s*type\\s*.*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), matchesRegex(".*\\s*LAST\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LAST_VALUE\\s*\\|\\s*AGGREGATE\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LEAST\\s*\\|\\s*CONDITIONAL\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LOG\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LOG10\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LCASE\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LEFT\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LENGTH\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LOCATE\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*LTRIM\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), is(emptyString()));
    }

    public void testShowFunctionsLikeInfix() throws IOException {
        assertThat(command("SHOW FUNCTIONS LIKE '%DAY%'"), matchesRegex(".*\\s*name\\s*\\|\\s*type\\s*.*"));
        assertThat(readLine(), containsString(HEADER_SEPARATOR));
        assertThat(readLine(), matchesRegex(".*\\s*DAY\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAYNAME\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAYOFMONTH\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAYOFWEEK\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAYOFYEAR\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAY_NAME\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAY_OF_MONTH\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAY_OF_WEEK\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*DAY_OF_YEAR\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*HOUR_OF_DAY\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*ISODAYOFWEEK\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*ISO_DAY_OF_WEEK\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*MINUTE_OF_DAY\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), matchesRegex(".*\\s*TODAY\\s*\\|\\s*SCALAR\\s*.*"));
        assertThat(readLine(), is(emptyString()));
    }
}
