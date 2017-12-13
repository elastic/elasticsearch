/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.cli.CliHttpClient;
import org.elasticsearch.xpack.sql.cli.TestTerminal;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ServerInfoCliCommandTests extends ESTestCase {

    public void testInvalidCommand() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        CliHttpClient client = mock(CliHttpClient.class);
        CliSession cliSession = new CliSession(client);
        ServerInfoCliCommand cliCommand = new ServerInfoCliCommand();
        assertFalse(cliCommand.handle(testTerminal, cliSession, "blah"));
        assertEquals(testTerminal.toString(), "");
        verifyNoMoreInteractions(client);
    }

    public void testShowInfo() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        CliHttpClient client = mock(CliHttpClient.class);
        CliSession cliSession = new CliSession(client);
        when(client.serverInfo()).thenReturn(new InfoResponse("my_node", "my_cluster", (byte) 1, (byte) 2, "v1.2", "1234", "Sep 1, 2017"));
        ServerInfoCliCommand cliCommand = new ServerInfoCliCommand();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "info"));
        assertEquals(testTerminal.toString(), "Node:<em>my_node</em> Cluster:<em>my_cluster</em> Version:<em>v1.2</em>\n");
        verify(client, times(1)).serverInfo();
        verifyNoMoreInteractions(client);
    }

}