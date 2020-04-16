/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.client.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.client.SslConfig;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ConnectionBuilderTests extends ESTestCase {

    public void testDefaultConnection() throws Exception {
        CliTerminal testTerminal = mock(CliTerminal.class);
        ConnectionBuilder connectionBuilder = new ConnectionBuilder(testTerminal);
        boolean binaryCommunication = randomBoolean();
        ConnectionConfiguration con = connectionBuilder.buildConnection(null, null, binaryCommunication);
        assertNull(con.authUser());
        assertNull(con.authPass());
        assertEquals("http://localhost:9200/", con.connectionString());
        assertEquals(URI.create("http://localhost:9200/"), con.baseUri());
        assertEquals(30000, con.connectTimeout());
        assertEquals(60000, con.networkTimeout());
        assertEquals(45000, con.pageTimeout());
        assertEquals(90000, con.queryTimeout());
        assertEquals(1000, con.pageSize());
        assertEquals(binaryCommunication, con.binaryCommunication());
        verifyNoMoreInteractions(testTerminal);
    }

    public void testBasicConnection() throws Exception {
        CliTerminal testTerminal = mock(CliTerminal.class);
        ConnectionBuilder connectionBuilder = new ConnectionBuilder(testTerminal);
        ConnectionConfiguration con = buildConnection(connectionBuilder, "http://foobar:9242/", null);
        assertNull(con.authUser());
        assertNull(con.authPass());
        assertEquals("http://foobar:9242/", con.connectionString());
        assertEquals(URI.create("http://foobar:9242/"), con.baseUri());
        verifyNoMoreInteractions(testTerminal);
    }

    public void testUserAndPasswordConnection() throws Exception {
        CliTerminal testTerminal = mock(CliTerminal.class);
        ConnectionBuilder connectionBuilder = new ConnectionBuilder(testTerminal);
        ConnectionConfiguration con = buildConnection(connectionBuilder, "http://user:pass@foobar:9242/", null);
        assertEquals("user", con.authUser());
        assertEquals("pass", con.authPass());
        assertEquals("http://user:pass@foobar:9242/", con.connectionString());
        assertEquals(URI.create("http://foobar:9242/"), con.baseUri());
        verifyNoMoreInteractions(testTerminal);
    }

    public void testAskUserForPassword() throws Exception {
        CliTerminal testTerminal = mock(CliTerminal.class);
        when(testTerminal.readPassword("password: ")).thenReturn("password");
        ConnectionBuilder connectionBuilder = new ConnectionBuilder(testTerminal);
        ConnectionConfiguration con = buildConnection(connectionBuilder, "http://user@foobar:9242/", null);
        assertEquals("user", con.authUser());
        assertEquals("password", con.authPass());
        assertEquals("http://user@foobar:9242/", con.connectionString());
        assertEquals(URI.create("http://foobar:9242/"), con.baseUri());
        verify(testTerminal, times(1)).readPassword(any());
        verifyNoMoreInteractions(testTerminal);
    }

    public void testAskUserForPasswordAndKeystorePassword() throws Exception {
        CliTerminal testTerminal = mock(CliTerminal.class);
        when(testTerminal.readPassword("keystore password: ")).thenReturn("keystore password");
        when(testTerminal.readPassword("password: ")).thenReturn("password");
        AtomicBoolean called = new AtomicBoolean(false);
        ConnectionBuilder connectionBuilder = new ConnectionBuilder(testTerminal) {
            @Override
            protected void checkIfExists(String name, Path p) {
                // Stubbed so we don't need permission to read the file
            }

            @Override
            protected ConnectionConfiguration newConnectionConfiguration(URI uri, String connectionString,
                    Properties properties) {
                // Stub building the actual configuration because we don't have permission to read the keystore.
                assertEquals("true", properties.get(SslConfig.SSL));
                assertEquals("keystore_location", properties.get(SslConfig.SSL_KEYSTORE_LOCATION));
                assertEquals("keystore password", properties.get(SslConfig.SSL_KEYSTORE_PASS));
                assertEquals("keystore_location", properties.get(SslConfig.SSL_TRUSTSTORE_LOCATION));
                assertEquals("keystore password", properties.get(SslConfig.SSL_TRUSTSTORE_PASS));

                called.set(true);
                return null;
            }
        };
        assertNull(buildConnection(connectionBuilder, "https://user@foobar:9242/", "keystore_location"));
        assertTrue(called.get());
        verify(testTerminal, times(2)).readPassword(any());
        verifyNoMoreInteractions(testTerminal);
    }

    public void testUserGaveUpOnPassword() throws Exception {
        CliTerminal testTerminal = mock(CliTerminal.class);
        UserException ue = new UserException(randomInt(), randomAlphaOfLength(5));
        when(testTerminal.readPassword("password: ")).thenThrow(ue);
        ConnectionBuilder connectionBuilder = new ConnectionBuilder(testTerminal);
        UserException actual = expectThrows(UserException.class, () ->
            buildConnection(connectionBuilder, "http://user@foobar:9242/", null));
        assertSame(actual, ue);
    }

    public void testUserGaveUpOnKeystorePassword() throws Exception {
        CliTerminal testTerminal = mock(CliTerminal.class);
        UserException ue = new UserException(randomInt(), randomAlphaOfLength(5));
        when(testTerminal.readPassword("keystore password: ")).thenThrow(ue);
        when(testTerminal.readPassword("password: ")).thenReturn("password");
        ConnectionBuilder connectionBuilder = new ConnectionBuilder(testTerminal) {
            @Override
            protected void checkIfExists(String name, Path p) {
                // Stubbed so we don't need permission to read the file
            }
        };
        UserException actual = expectThrows(UserException.class, () ->
            buildConnection(connectionBuilder, "https://user@foobar:9242/", "keystore_location"));
        assertSame(actual, ue);
    }
    
    private ConnectionConfiguration buildConnection(ConnectionBuilder builder, String connectionStringArg,
                                                    String keystoreLocation) throws UserException {
        return builder.buildConnection(connectionStringArg, keystoreLocation, randomBoolean());
    }
}
