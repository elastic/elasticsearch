/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli.SecurityConfig;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.xpack.ql.TestUtils.assertNoSearchContexts;
import static org.elasticsearch.xpack.sql.qa.rest.RemoteClusterAwareSqlRestTestCase.AUTH_PASS;
import static org.elasticsearch.xpack.sql.qa.rest.RemoteClusterAwareSqlRestTestCase.AUTH_USER;

public abstract class CliIntegrationTestCase extends ESRestTestCase {
    /**
     * Read an address for Elasticsearch suitable for the CLI from the system properties.
     */
    public String elasticsearchAddress() {
        // CLI only supports a single node at a time so we just give it one.
        return getTestRestCluster().split(",")[0];
    }

    private EmbeddedCli cli;

    /**
     * Asks the CLI Fixture to start a CLI instance.
     */
    @Before
    public void startCli() throws IOException {
        cli = new EmbeddedCli(elasticsearchAddress(), true, securityConfig());
    }

    @After
    public void orderlyShutdown() throws Exception {
        if (cli == null) {
            // failed to connect to the cli so there is nothing to do here
            return;
        }
        cli.close();
        assertNoSearchContexts(client());
    }

    /**
     * Override to add security configuration to the cli.
     */
    protected SecurityConfig securityConfig() {
        if (hasText(AUTH_USER) && hasText(AUTH_PASS)) {
            return new SecurityConfig(false, AUTH_USER, AUTH_PASS, null, null);
        }

        return null;
    }

    protected void index(String index, int docId, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + docId);
        request.addParameter("refresh", "true");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        request.setJsonEntity(Strings.toString(builder));
        client().performRequest(request);
    }

    protected void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        index(index, 1, body);
    }

    public String command(String command) throws IOException {
        return cli.command(command);
    }

    /**
     * Read a line produced by the CLI.
     * Note that these lines will contain {@code xterm-256color}
     * escape sequences.
     */
    public String readLine() throws IOException {
        return cli.readLine();
    }

}
