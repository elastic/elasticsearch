/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.xpack.sql.cli.command.CliSession;
import org.elasticsearch.xpack.sql.client.ClientException;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersions;

import java.sql.SQLException;

import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_VERSION_COMPATIBILITY;
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
        var preVersionCompat = SqlVersions.getAllVersions()
            .stream()
            .filter(v -> v.onOrAfter(INTRODUCING_VERSION_COMPATIBILITY) == false)
            .toList();
        SqlVersion version = preVersionCompat.get(random().nextInt(preVersionCompat.size()));
        when(httpClient.serverInfo()).thenReturn(
            new MainResponse(randomAlphaOfLength(5), version.toString(), ClusterName.DEFAULT.value(), UUIDs.randomBase64UUID())
        );
        CliSession cliSession = new CliSession(httpClient);
        Throwable throwable = expectThrows(ClientException.class, cliSession::checkConnection);
        assertEquals(
            "This version of the CLI is only compatible with Elasticsearch version "
                + ClientVersion.CURRENT.majorMinorToString()
                + " or newer; attempting to connect to a server version "
                + version,
            throwable.getMessage()
        );
        verify(httpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(httpClient);
    }

    public void testHigherServerVersion() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        var postVersionCompat = SqlVersions.getAllVersions().stream().filter(v -> v.onOrAfter(INTRODUCING_VERSION_COMPATIBILITY)).toList();
        SqlVersion version = postVersionCompat.get(random().nextInt(postVersionCompat.size()));
        when(httpClient.serverInfo()).thenReturn(
            new MainResponse(randomAlphaOfLength(5), version.toString(), ClusterName.DEFAULT.value(), UUIDs.randomBase64UUID())
        );
        CliSession cliSession = new CliSession(httpClient);
        cliSession.checkConnection();
        verify(httpClient, times(1)).serverInfo();
        verifyNoMoreInteractions(httpClient);
    }
}
