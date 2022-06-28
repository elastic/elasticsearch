/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.xpack.sql.cli.SqlCliTestCase;
import org.elasticsearch.xpack.sql.cli.TestTerminal;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.proto.MainResponse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ServerInfoCliCommandTests extends SqlCliTestCase {

    public void testInvalidCommand() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient client = mock(HttpClient.class);
        CliSession cliSession = new CliSession(client);
        ServerInfoCliCommand cliCommand = new ServerInfoCliCommand();
        assertFalse(cliCommand.handle(testTerminal, cliSession, "blah"));
        assertEquals(testTerminal.toString(), "");
        verifyNoMoreInteractions(client);
    }

    public void testShowInfo() throws Exception {
        TestTerminal testTerminal = new TestTerminal();
        HttpClient client = mock(HttpClient.class);
        CliSession cliSession = new CliSession(client);
        when(client.serverInfo()).thenReturn(
            new MainResponse("my_node", "1.2.3", new ClusterName("my_cluster").value(), UUIDs.randomBase64UUID())
        );
        ServerInfoCliCommand cliCommand = new ServerInfoCliCommand();
        assertTrue(cliCommand.handle(testTerminal, cliSession, "info"));
        assertEquals(testTerminal.toString(), "Node:<em>my_node</em> Cluster:<em>my_cluster</em> Version:<em>1.2.3</em>\n");
        verify(client, times(1)).serverInfo();
        verifyNoMoreInteractions(client);
    }

}
