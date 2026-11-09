/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient.ConnectedSocketFactory;
import org.elasticsearch.xpack.core.ssl.SslSettingsLoader;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ConnectedSocketFactoryTests extends ESTestCase {

    private MockWebServer server;
    private SSLSocketFactory clientSocketFactory;

    @Before
    public void startServer() throws Exception {
        final Path caCertPath = getDataPath("/org/elasticsearch/xpack/core/ssl/ca/ca.crt");
        final Path certPath = getDataPath("/org/elasticsearch/xpack/core/ssl/server/server.crt");
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/core/ssl/server/server.key");

        final Settings serverSettings = Settings.builder()
            .put("ssl.certificate", certPath)
            .put("ssl.key", keyPath)
            .put("ssl.client_authentication", "none")
            .build();
        final SslConfiguration serverSsl = SslSettingsLoader.load(serverSettings, "ssl.", newEnvironment());
        server = new MockWebServer(serverSsl.createSslContext(), new MockWebServer.TlsConfig(serverSsl));
        // The test certificate only has SANs for 127.0.0.1 and localhost
        server.start(InetAddress.getByName("127.0.0.1"));

        final Settings clientSettings = Settings.builder()
            .put("xpack.http.ssl.certificate_authorities", caCertPath)
            .put("xpack.http.ssl.verification_mode", "full")
            .build();
        clientSocketFactory = new TestsSSLService(newEnvironment(clientSettings)).profile("xpack.http.ssl").socketFactory();
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    /**
     * The property a JSSE provider without access to the JDK-internal host hook (such as Bouncy Castle's) depends on: the TLS socket is
     * layered over a socket that is already connected, and is given the host name from the URL for endpoint identification.
     */
    public void testLayersTlsOverConnectedSocketWithUrlHost() throws Exception {
        final String host = randomFrom("localhost", "127.0.0.1");
        final RecordingSocketFactory recording = new RecordingSocketFactory(clientSocketFactory);

        assertThat(get(host, new ConnectedSocketFactory(recording)), equalTo(200));
        assertThat(recording.calls, contains("layered:" + host + ":" + server.getPort() + ":connected"));
    }

    /**
     * Guards the premise of {@link ConnectedSocketFactory}: left to itself, {@link HttpsURLConnection} asks for an unconnected socket and
     * leaves the provider to recover the peer name on its own. If this stops holding, the wrapper is no longer needed.
     */
    public void testUnwrappedFactoryIsAskedForUnconnectedSocket() throws Exception {
        final RecordingSocketFactory recording = new RecordingSocketFactory(clientSocketFactory);

        assertThat(get("localhost", recording), equalTo(200));
        assertThat(recording.calls, contains("unconnected"));
    }

    /**
     * {@link HttpsURLConnection} only falls back to connecting a plain socket itself when the factory refuses to create an unconnected
     * socket with exactly this exception.
     */
    public void testDeclinesUnconnectedSockets() {
        final ConnectedSocketFactory factory = new ConnectedSocketFactory(clientSocketFactory);
        final SocketException e = expectThrows(SocketException.class, factory::createSocket);
        assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
    }

    @SuppressForbidden(reason = "HttpsURLConnection is the client whose socket creation is under test")
    private int get(String host, SSLSocketFactory socketFactory) throws IOException {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        final URL url = URI.create("https://" + host + ":" + server.getPort() + "/").toURL();
        final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        try {
            connection.setSSLSocketFactory(socketFactory);
            connection.connect();
            return connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }

    /**
     * Records which of the factory methods {@link HttpsURLConnection} uses, and with what host, before delegating to a real factory.
     * A subclass rather than a mock, since the connection must still complete a real handshake.
     */
    private static final class RecordingSocketFactory extends SSLSocketFactory {

        private final SSLSocketFactory delegate;
        private final List<String> calls = new CopyOnWriteArrayList<>();

        RecordingSocketFactory(SSLSocketFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public Socket createSocket() throws IOException {
            calls.add("unconnected");
            return delegate.createSocket();
        }

        @Override
        public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
            calls.add("layered:" + host + ":" + port + ":" + (socket.isConnected() ? "connected" : "unconnected"));
            return delegate.createSocket(socket, host, port, autoClose);
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            calls.add("host:" + host + ":" + port);
            return delegate.createSocket(host, port);
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            calls.add("host:" + host + ":" + port);
            return delegate.createSocket(host, port, localHost, localPort);
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            calls.add("address:" + host + ":" + port);
            return delegate.createSocket(host, port);
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            calls.add("address:" + address + ":" + port);
            return delegate.createSocket(address, port, localAddress, localPort);
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }
    }
}
