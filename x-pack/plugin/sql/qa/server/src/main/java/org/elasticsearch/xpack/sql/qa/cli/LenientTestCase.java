/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesRegex;

public abstract class LenientTestCase extends CliIntegrationTestCase {

    public void testLenientCommand() throws IOException {
        index("test", body -> body.field("name", "foo").field("tags", new String[] { "bar", "bar" }));
        assertEquals("[?1l>[?1000l[?2004llenient set to [90mtrue[0m", command("lenient = true"));
        assertThat(command("SELECT * FROM test"), matchesRegex(".*\\s*name\\s*\\|\\s*tags\\s*.*"));
        assertThat(readLine(), containsString("----------"));
        assertThat(readLine(), matchesRegex(".*\\s*foo\\s*\\|\\s*bar\\s*.*"));
        assertEquals("", readLine());
    }

    public void testDefaultNoLenient() throws IOException {
        index("test", body -> body.field("name", "foo").field("tags", new String[] { "bar", "bar" }));
        assertThat(
            command("SELECT * FROM test"),
            containsString("[?1l>[?1000l[?2004l[31;1mBad request [[3;33;22mArrays (returned by [tags]) are not supported[23;31;1m][0m")
        );
    }

    public void testExplicitNoLenient() throws IOException {
        index("test", body -> body.field("name", "foo").field("tags", new String[] { "bar", "bar" }));
        assertEquals("[?1l>[?1000l[?2004llenient set to [90mfalse[0m", command("lenient = false"));
        assertThat(
            command("SELECT * FROM test"),
            containsString("[?1l>[?1000l[?2004l[31;1mBad request [[3;33;22mArrays (returned by [tags]) are not supported[23;31;1m][0m")
        );
    }
}
