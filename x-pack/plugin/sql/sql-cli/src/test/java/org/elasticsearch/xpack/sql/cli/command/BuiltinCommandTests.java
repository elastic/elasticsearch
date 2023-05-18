/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.SqlCliTestCase;
import org.elasticsearch.xpack.sql.cli.TestTerminal;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.HttpClient;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class BuiltinCommandTests extends SqlCliTestCase {

    public void testInvalidCommand() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        assertFalse(new ClearScreenCliCommand().handle(testTerminal, cliSession, "something"));
        assertFalse(new FetchSeparatorCliCommand().handle(testTerminal, cliSession, "something"));
        assertFalse(new FetchSizeCliCommand().handle(testTerminal, cliSession, "something"));
        assertFalse(new PrintLogoCommand().handle(testTerminal, cliSession, "something"));
        verifyNoMoreInteractions(httpClient);
    }

    public void testClearScreen() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        testTerminal.print("not clean");
        assertTrue(new ClearScreenCliCommand().handle(testTerminal, cliSession, "cls"));
        assertEquals("", testTerminal.toString());
        verifyNoMoreInteractions(httpClient);
    }

    public void testFetchSeparator() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        FetchSeparatorCliCommand cliCommand = new FetchSeparatorCliCommand();
        assertFalse(cliCommand.handle(testTerminal, cliSession, "fetch"));
        assertEquals("", cliSession.cfg().getFetchSeparator());

        assertTrue(cliCommand.handle(testTerminal, cliSession, "fetch_separator = \"foo\""));
        assertEquals("foo", cliSession.cfg().getFetchSeparator());
        assertEquals("fetch separator set to \"<em>foo</em>\"<flush/>", testTerminal.toString());
        testTerminal.clear();

        assertTrue(cliCommand.handle(testTerminal, cliSession, "fetch_separator=\"bar\""));
        assertEquals("bar", cliSession.cfg().getFetchSeparator());
        assertEquals("fetch separator set to \"<em>bar</em>\"<flush/>", testTerminal.toString());
        testTerminal.clear();

        assertTrue(cliCommand.handle(testTerminal, cliSession, "fetch separator=\"baz\""));
        assertEquals("baz", cliSession.cfg().getFetchSeparator());
        assertEquals("fetch separator set to \"<em>baz</em>\"<flush/>", testTerminal.toString());
        verifyNoMoreInteractions(httpClient);
    }

    public void testFetchSize() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        FetchSizeCliCommand cliCommand = new FetchSizeCliCommand();
        assertFalse(cliCommand.handle(testTerminal, cliSession, "fetch"));
        assertEquals(1000L, cliSession.cfg().getFetchSize());

        assertTrue(cliCommand.handle(testTerminal, cliSession, "fetch_size = \"foo\""));
        assertEquals(1000L, cliSession.cfg().getFetchSize());
        assertEquals("<b>Invalid fetch size [</b><i>\"foo\"</i><b>]</b><flush/>", testTerminal.toString());
        testTerminal.clear();

        assertTrue(cliCommand.handle(testTerminal, cliSession, "fetch_size = 10"));
        assertEquals(10L, cliSession.cfg().getFetchSize());
        assertEquals("fetch size set to <em>10</em><flush/>", testTerminal.toString());

        testTerminal.clear();

        assertTrue(cliCommand.handle(testTerminal, cliSession, "fetch_size = -10"));
        assertEquals(10L, cliSession.cfg().getFetchSize());
        assertEquals("<b>Invalid fetch size [</b><i>-10</i><b>]. Must be > 0.</b><flush/>", testTerminal.toString());
        verifyNoMoreInteractions(httpClient);
    }

    public void testPrintLogo() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        testTerminal.print("not clean");
        assertTrue(new PrintLogoCommand().handle(testTerminal, cliSession, "logo"));
        assertThat(testTerminal.toString(), containsString("SQL"));
        assertThat(testTerminal.toString(), containsString(ClientVersion.CURRENT.version));
        verifyNoMoreInteractions(httpClient);
    }

    public void testLenient() {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        LenientCliCommand cliCommand = new LenientCliCommand();
        assertFalse(cliCommand.handle(testTerminal, cliSession, "lenient"));
        assertEquals(false, cliSession.cfg().isLenient());
        assertTrue(cliCommand.handle(testTerminal, cliSession, "lenient = true"));
        assertEquals(true, cliSession.cfg().isLenient());
        assertEquals("lenient set to <em>true</em><flush/>", testTerminal.toString());
        testTerminal.clear();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "lenient = false"));
        assertEquals(false, cliSession.cfg().isLenient());
        assertEquals("lenient set to <em>false</em><flush/>", testTerminal.toString());
        testTerminal.clear();
    }

    public void testAllowPartialSearchResults() {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient httpClient = mock(HttpClient.class);
        CliSession cliSession = new CliSession(httpClient);
        AllowPartialResultsCliCommand cliCommand = new AllowPartialResultsCliCommand();
        assertFalse(cliCommand.handle(testTerminal, cliSession, "allow_partial_search_results"));
        assertEquals(false, cliSession.cfg().allowPartialResults());
        assertTrue(cliCommand.handle(testTerminal, cliSession, "allow_partial_search_results = true"));
        assertEquals(true, cliSession.cfg().allowPartialResults());
        assertEquals("allow_partial_search_results set to <em>true</em><flush/>", testTerminal.toString());
        testTerminal.clear();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "allow_partial_search_results = false"));
        assertEquals(false, cliSession.cfg().allowPartialResults());
        assertEquals("allow_partial_search_results set to <em>false</em><flush/>", testTerminal.toString());
        testTerminal.clear();
    }

}
