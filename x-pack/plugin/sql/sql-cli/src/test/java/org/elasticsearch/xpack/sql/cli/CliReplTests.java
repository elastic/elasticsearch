/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.cli.command.CliCommand;
import org.elasticsearch.xpack.sql.cli.command.CliSession;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CliReplTests extends ESTestCase {

    public void testBasicCliFunctionality() throws Exception {
        CliTerminal cliTerminal = new TestTerminal(
                "test;",
                "notest;",
                "exit;"
        );
        CliSession mockSession = mock(CliSession.class);
        CliCommand mockCommand = mock(CliCommand.class);
        when(mockCommand.handle(cliTerminal, mockSession, "logo")).thenReturn(true);
        when(mockCommand.handle(cliTerminal, mockSession, "test")).thenReturn(true);
        when(mockCommand.handle(cliTerminal, mockSession, "notest")).thenReturn(false);

        CliRepl cli = new CliRepl(cliTerminal, mockSession, mockCommand);
        cli.execute();

        verify(mockCommand, times(1)).handle(cliTerminal, mockSession, "test");
        verify(mockCommand, times(1)).handle(cliTerminal, mockSession, "logo");
        verify(mockCommand, times(1)).handle(cliTerminal, mockSession, "notest");
        verifyNoMoreInteractions(mockCommand, mockSession);
    }

    /**
     * Test that empty commands are skipped. This includes commands that are
     * just new lines.
     */
    public void testEmptyNotSent() {
        CliTerminal cliTerminal = new TestTerminal(
                ";",
                "",
                "",
                ";",
                "exit;"
        );

        CliSession mockSession = mock(CliSession.class);
        CliCommand mockCommand = mock(CliCommand.class);

        CliRepl cli = new CliRepl(cliTerminal, mockSession, mockCommand);
        cli.execute();

        verify(mockCommand, times(1)).handle(cliTerminal, mockSession, "logo");
        verifyNoMoreInteractions(mockSession, mockCommand);
    }

    public void testFatalCliExceptionHandling() throws Exception {
        CliTerminal cliTerminal = new TestTerminal(
                "test;",
                "fail;"
        );

        CliSession mockSession = mock(CliSession.class);
        CliCommand mockCommand = mock(CliCommand.class);
        when(mockCommand.handle(cliTerminal, mockSession, "logo")).thenReturn(true);
        when(mockCommand.handle(cliTerminal, mockSession, "test")).thenReturn(true);
        when(mockCommand.handle(cliTerminal, mockSession, "fail")).thenThrow(new FatalCliException("die"));

        CliRepl cli = new CliRepl(cliTerminal, mockSession, mockCommand);
        expectThrows(FatalCliException.class, cli::execute);

        verify(mockCommand, times(1)).handle(cliTerminal, mockSession, "logo");
        verify(mockCommand, times(1)).handle(cliTerminal, mockSession, "test");
        verify(mockCommand, times(1)).handle(cliTerminal, mockSession, "fail");
        verifyNoMoreInteractions(mockCommand, mockSession);
    }

}
