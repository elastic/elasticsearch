/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.sql.cli.command.CliSession;
import org.elasticsearch.xpack.sql.client.ClientException;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CliSessionTests extends SqlCliTestCase {

    public void testProperConnection() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.serverInfo()).thenReturn(
            new MainResponse(
                randomAlphaOfLength(5),
                ClientVersion.CURRENT.toString(),
                ClusterName.DEFAULT.value(),
                UUIDs.randomBase64UUID()
            )
        );
        CliSession cliSession = new CliSession(httpClient);
        cliSession.checkConnection();
        verify(httpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(httpClient);
    }

    public void testConnection() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.serverInfo()).thenThrow(new SQLException("Cannot connect"));
        CliSession cliSession = new CliSession(httpClient);
        Throwable throwable = expectThrows(ClientException.class, cliSession::checkConnection);
        assertEquals("java.sql.SQLException: Cannot connect", throwable.getMessage());
        verify(httpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(httpClient);
    }

    public void testWrongServerVersion() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        Version v = VersionUtils.randomVersionBetween(random(), null, VersionUtils.getPreviousVersion(Version.V_7_7_0));
        SqlVersion version = new SqlVersion(v.major, v.minor, v.revision);
        when(httpClient.serverInfo()).thenReturn(
            new MainResponse(randomAlphaOfLength(5), version.toString(), ClusterName.DEFAULT.value(), UUIDs.randomBase64UUID())
        );
        CliSession cliSession = new CliSession(httpClient);
        Throwable throwable = expectThrows(ClientException.class, cliSession::checkConnection);
        assertEquals(
            "This version of the CLI is only compatible with Elasticsearch version "
                + ClientVersion.CURRENT.majorMinorToString()
                + " or newer; attempting to connect to a server version "
                + version.toString(),
            throwable.getMessage()
        );
        verify(httpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(httpClient);
    }

    public void testHigherServerVersion() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        Version v = VersionUtils.randomVersionBetween(random(), Version.V_7_7_0, null);
        SqlVersion version = new SqlVersion(v.major, v.minor, v.revision);
        when(httpClient.serverInfo()).thenReturn(
            new MainResponse(randomAlphaOfLength(5), version.toString(), ClusterName.DEFAULT.value(), UUIDs.randomBase64UUID())
        );
        CliSession cliSession = new CliSession(httpClient);
        cliSession.checkConnection();
        verify(httpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(httpClient);
    }
}
