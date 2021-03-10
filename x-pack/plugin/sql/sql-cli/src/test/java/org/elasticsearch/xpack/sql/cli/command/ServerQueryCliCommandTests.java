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
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ServerQueryCliCommandTests extends SqlCliTestCase {

    public void testExceptionHandling() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient client = mock(HttpClient.class);
        CliSession cliSession = new CliSession(client);
        when(client.basicQuery("blah", 1000)).thenThrow(new SQLException("test exception"));
        ServerQueryCliCommand cliCommand = new ServerQueryCliCommand();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "blah"));
        assertEquals("<b>Bad request [</b><i>test exception</i><b>]</b>\n", testTerminal.toString());
        verify(client, times(1)).basicQuery(eq("blah"), eq(1000));
        verifyNoMoreInteractions(client);
    }

    public void testOnePageQuery() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient client = mock(HttpClient.class);
        CliSession cliSession = new CliSession(client);
        cliSession.setFetchSize(10);
        when(client.basicQuery("test query", 10)).thenReturn(fakeResponse("", true, "foo"));
        ServerQueryCliCommand cliCommand = new ServerQueryCliCommand();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "test query"));
        assertEquals("     field     \n---------------\nfoo            \n<flush/>", testTerminal.toString());
        verify(client, times(1)).basicQuery(eq("test query"), eq(10));
        verifyNoMoreInteractions(client);
    }

    public void testThreePageQuery() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient client = mock(HttpClient.class);
        CliSession cliSession = new CliSession(client);
        cliSession.setFetchSize(10);
        when(client.basicQuery("test query", 10)).thenReturn(fakeResponse("my_cursor1", true, "first"));
        when(client.nextPage("my_cursor1")).thenReturn(fakeResponse("my_cursor2", false, "second"));
        when(client.nextPage("my_cursor2")).thenReturn(fakeResponse("", false, "third"));
        ServerQueryCliCommand cliCommand = new ServerQueryCliCommand();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "test query"));
        assertEquals("     field     \n---------------\nfirst          \nsecond         \nthird          \n<flush/>",
                testTerminal.toString());
        verify(client, times(1)).basicQuery(eq("test query"), eq(10));
        verify(client, times(2)).nextPage(any());
        verifyNoMoreInteractions(client);
    }

    public void testTwoPageQueryWithSeparator() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient client = mock(HttpClient.class);
        CliSession cliSession = new CliSession(client);
        cliSession.setFetchSize(15);
        // Set a separator
        cliSession.setFetchSeparator("-----");
        when(client.basicQuery("test query", 15)).thenReturn(fakeResponse("my_cursor1", true, "first"));
        when(client.nextPage("my_cursor1")).thenReturn(fakeResponse("", false, "second"));
        ServerQueryCliCommand cliCommand = new ServerQueryCliCommand();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "test query"));
        assertEquals("     field     \n---------------\nfirst          \n-----\nsecond         \n<flush/>",
                testTerminal.toString());
        verify(client, times(1)).basicQuery(eq("test query"), eq(15));
        verify(client, times(1)).nextPage(any());
        verifyNoMoreInteractions(client);
    }

    public void testCursorCleanupOnError() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient client = mock(HttpClient.class);
        CliSession cliSession = new CliSession(client);
        cliSession.setFetchSize(15);
        when(client.basicQuery("test query", 15)).thenReturn(fakeResponse("my_cursor1", true, "first"));
        when(client.nextPage("my_cursor1")).thenThrow(new SQLException("test exception"));
        when(client.queryClose("my_cursor1", Mode.CLI)).thenReturn(true);
        ServerQueryCliCommand cliCommand = new ServerQueryCliCommand();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "test query"));
        assertEquals("     field     \n---------------\nfirst          \n" +
                "<b>Bad request [</b><i>test exception</i><b>]</b>\n", testTerminal.toString());
        verify(client, times(1)).basicQuery(eq("test query"), eq(15));
        verify(client, times(1)).nextPage(any());
        verify(client, times(1)).queryClose(eq("my_cursor1"), eq(Mode.CLI));
        verifyNoMoreInteractions(client);
    }

    private SqlQueryResponse fakeResponse(String cursor, boolean includeColumns, String val) {
        List<List<Object>> rows;
        List<ColumnInfo> columns;
        if (includeColumns) {
            columns = singletonList(new ColumnInfo("", "field", "string", 0));
        } else {
            columns = null;
        }
        if (val != null) {
            rows = singletonList(Collections.singletonList(val));
        } else {
            rows = singletonList(Collections.emptyList());
        }
        return new SqlQueryResponse(cursor, columns, rows);
    }
}
