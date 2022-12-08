package org.elasticsearch.repositories.gcs;

import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class MockHttpProxyServerTest extends ESTestCase {

    public void testProxyServerWorks() throws Exception {
        String httpBody = randomAlphaOfLength(32);
        var proxyServer = new MockHttpProxyServer((reader, writer) -> {
            assertEquals("GET http://googleapis.com HTTP/1.1", reader.readLine());
            writer.write(formatted("""
                HTTP/1.1 200 OK\r
                Content-Length: %s\r
                \r
                %s""", httpBody.length(), httpBody));
        }).await();
        try (proxyServer) {
            var urlConnection = SocketAccess.doPrivilegedIOException(() -> {
                var connection = new URL("http://googleapis.com").openConnection(
                    new Proxy(Proxy.Type.HTTP, new InetSocketAddress(InetAddress.getLoopbackAddress(), proxyServer.getPort()))
                );
                connection.connect();
                return connection;
            });
            assertEquals(httpBody.length(), urlConnection.getContentLengthLong());
            try (var bodyReader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), StandardCharsets.UTF_8))) {
                assertEquals(httpBody, bodyReader.readLine());
            }
        }
    }
}
