package org.elasticsearch.repositories.gcs;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;

public class MockHttpProxyServerTest extends ESTestCase {

    public void testProxyServerWorks() throws Exception {
        String httpBody = randomAlphaOfLength(32);
        var proxyServer = new MockHttpProxyServer((reader, writer) -> {
            assertEquals("GET http://googleapis.com/ HTTP/1.1", reader.readLine());
            writer.write(formatted("""
                HTTP/1.1 200 OK\r
                Content-Length: %s\r
                \r
                %s""", httpBody.length(), httpBody));
        }).await();
        var httpClient = HttpClients.custom()
            .setRoutePlanner(new DefaultProxyRoutePlanner(new HttpHost(InetAddress.getLoopbackAddress(), proxyServer.getPort())))
            .build();
        try (
            proxyServer;
            httpClient;
            var httpResponse = SocketAccess.doPrivilegedIOException(() -> httpClient.execute(new HttpGet("http://googleapis.com/")))
        ) {
            assertEquals(httpBody.length(), httpResponse.getEntity().getContentLength());
            assertEquals(httpBody, EntityUtils.toString(httpResponse.getEntity()));
        }
    }
}
