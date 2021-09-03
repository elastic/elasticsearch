/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.SqlCliTestCase;
import org.elasticsearch.xpack.sql.cli.TestTerminal;
import org.elasticsearch.xpack.sql.client.HttpClient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CliCommandsTests extends SqlCliTestCase {

    public void testCliCommands() {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        CliCommands cliCommands = new CliCommands(
                (terminal, session, line) -> line.equals("foo"),
                (terminal, session, line) -> line.equals("bar"),
                (terminal, session, line) -> line.equals("baz")
        );

        assertTrue(cliCommands.handle(testTerminal, cliSession, "foo"));
        assertTrue(cliCommands.handle(testTerminal, cliSession, "bar"));
        assertTrue(cliCommands.handle(testTerminal, cliSession, "baz"));
        assertFalse(cliCommands.handle(testTerminal, cliSession, ""));
        assertFalse(cliCommands.handle(testTerminal, cliSession, "something"));
        verifyNoMoreInteractions(httpClient);
    }
}
