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

public abstract class ProjectRoutingTestCase extends CliIntegrationTestCase {

    public void testProjectRoutingCommand() throws IOException {
        index("test", body -> body.field("name", "foo"));
        assertEquals("[?1l>[?1000l[?2004lproject_routing set to [90m_alias:foo[0m", command("project_routing = _alias:foo"));
        assertThat(
            command("SELECT * FROM test"),
            matchesRegex(".*\\s*\\[project_routing\\] is only allowed when cross-project search is enabled\\s*.*")
        );
    }

    public void testProjectRoutingResetCommand() throws IOException {
        index("test", body -> body.field("name", "foo"));
        assertEquals("[?1l>[?1000l[?2004lproject_routing set to [90m_alias:foo[0m", command("project_routing = _alias:foo"));
        assertEquals("[?1l>[?1000l[?2004lproject_routing set to [90mnull[0m", command("project_routing = null"));
        assertThat(command("SELECT * FROM test"), matchesRegex(".*\\s*name\\s*\\.*"));
        assertThat(readLine(), containsString("----------"));
        assertThat(readLine(), matchesRegex(".*\\s*foo\\s*\\.*"));
        assertEquals("", readLine());
    }
}
