/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.cli.command.CliSession;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.client.shared.ClientException;
import org.elasticsearch.xpack.sql.client.shared.Version;

import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CliSessionTests extends ESTestCase {

    public void testProperConnection() throws Exception {
        CliHttpClient cliHttpClient = mock(CliHttpClient.class);
        when(cliHttpClient.serverInfo()).thenReturn(new InfoResponse(randomAlphaOfLength(5), randomAlphaOfLength(5),
                (byte) Version.versionMajor(), (byte) Version.versionMinor(),
                randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5)));
        CliSession cliSession = new CliSession(cliHttpClient);
        cliSession.checkConnection();
        verify(cliHttpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(cliHttpClient);
    }

    public void testConnection() throws Exception {
        CliHttpClient cliHttpClient = mock(CliHttpClient.class);
        when(cliHttpClient.serverInfo()).thenThrow(new SQLException("Cannot connect"));
        CliSession cliSession = new CliSession(cliHttpClient);
        expectThrows(ClientException.class, cliSession::checkConnection);
        verify(cliHttpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(cliHttpClient);
    }

    public void testWrongServerVersion() throws Exception {
        CliHttpClient cliHttpClient = mock(CliHttpClient.class);
        byte minor;
        byte major;
        if (randomBoolean()) {
            minor = (byte) Version.versionMinor();
            major = (byte) (Version.versionMajor() + 1);
        } else {
            minor = (byte) (Version.versionMinor() + 1);
            major = (byte) Version.versionMajor();

        }
        when(cliHttpClient.serverInfo()).thenReturn(new InfoResponse(randomAlphaOfLength(5), randomAlphaOfLength(5),
                minor, major, randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5)));
        CliSession cliSession = new CliSession(cliHttpClient);
        expectThrows(ClientException.class, cliSession::checkConnection);
        verify(cliHttpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(cliHttpClient);
    }
}