/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_HOST_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_PORT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_TYPE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.BUCKET;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageRepository.CLIENT_NAME;

@SuppressForbidden(reason = "We start an HTTP proxy server to test proxy support for GCS")
public class GcsProxyIntegrationTests extends ESBlobStoreRepositoryIntegTestCase {

    private static HttpServer httpServer;
    private static MockHttpProxyServer proxyServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        proxyServer = new MockHttpProxyServer(new MockHttpProxyServer.SocketRequestHandler() {

            @Override
            @SuppressForbidden(reason = "Proxy makes requests to the upstream HTTP server")
            public void handle(InputStream is, OutputStream os) throws IOException {
                // We can't make a com.sun.net.httpserver act as an HTTP proxy, so we have to do work with
                // raw sockets and do HTTP parsing ourselves
                String requestLine = readLine(is);
                String[] requestLineParts = requestLine.split(" ");
                String requestMethod = requestLineParts[0];
                String url = requestLineParts[1];

                var upstreamHttpConnection = (HttpURLConnection) new URL(url).openConnection();
                upstreamHttpConnection.setRequestMethod(requestMethod);
                int requestContentLength = -1;
                boolean chunkedRequest = false;
                while (true) {
                    String requestHeader = readLine(is);
                    if (requestHeader.isEmpty()) {
                        break;
                    }
                    String[] headerParts = requestHeader.split(":");
                    String headerName = headerParts[0].trim();
                    String headerValue = headerParts[1].trim();
                    upstreamHttpConnection.setRequestProperty(headerName, headerValue);
                    if (headerName.equalsIgnoreCase("Content-Length")) {
                        requestContentLength = Integer.parseInt(headerValue);
                    } else if (headerName.equalsIgnoreCase("Transfer-Encoding") && headerValue.equalsIgnoreCase("chunked")) {
                        chunkedRequest = true;
                    }
                }
                if (requestContentLength > 0) {
                    upstreamHttpConnection.setDoOutput(true);
                    try (var uos = upstreamHttpConnection.getOutputStream()) {
                        uos.write(is.readNBytes(requestContentLength));
                    }
                } else if (chunkedRequest) {
                    upstreamHttpConnection.setDoOutput(true);
                    upstreamHttpConnection.setChunkedStreamingMode(0);
                    try (var uos = upstreamHttpConnection.getOutputStream()) {
                        while (true) {
                            String line = readLine(is);
                            if (line.isEmpty()) {
                                break;
                            }
                            int chunkSize = Integer.parseInt(line, 16);
                            if (chunkSize == 0) {
                                readCrlf(is);
                                break;
                            }
                            uos.write(is.readNBytes(chunkSize));
                            readCrlf(is);
                        }
                    }
                }
                upstreamHttpConnection.connect();

                String upstreamStatusLine = formatted(
                    "HTTP/1.1 %s %s\r\n",
                    upstreamHttpConnection.getResponseCode(),
                    upstreamHttpConnection.getResponseMessage()
                );
                os.write(upstreamStatusLine.getBytes(ISO_8859_1));
                StringBuilder responseHeaders = new StringBuilder();
                for (var upstreamHeader : upstreamHttpConnection.getHeaderFields().entrySet()) {
                    if (upstreamHeader.getKey() == null) {
                        continue;
                    }
                    responseHeaders.append(upstreamHeader.getKey()).append(": ");
                    for (int i = 0; i < upstreamHeader.getValue().size(); i++) {
                        responseHeaders.append(upstreamHeader.getValue().get(i));
                        if (i < upstreamHeader.getValue().size() - 1) {
                            responseHeaders.append(",");
                        }
                    }
                    responseHeaders.append("\r\n");
                }
                responseHeaders.append("\r\n");
                os.write(responseHeaders.toString().getBytes(ISO_8859_1));
                int upstreamContentLength = upstreamHttpConnection.getContentLength();
                if (upstreamContentLength > 0) {
                    try (var uis = upstreamHttpConnection.getInputStream()) {
                        os.write(uis.readNBytes(upstreamContentLength));
                    }
                }
            }

            private static String readLine(InputStream is) throws IOException {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                while (true) {
                    int b = is.read();
                    if (b == -1) {
                        break;
                    }
                    if (b == '\r') {
                        int n = is.read();
                        if (n != '\n') {
                            throw new IllegalStateException("Not CRLF");
                        }
                        break;
                    }
                    os.write(b);
                }
                return os.toString(ISO_8859_1);
            }

            private static void readCrlf(InputStream is) throws IOException {
                if (is.read() != '\r' || is.read() != '\n') {
                    throw new IllegalStateException("Not CRLF");
                }
            }

        }).await();
    }

    @AfterClass
    public static void stopHttpServer() throws IOException {
        httpServer.stop(0);
        proxyServer.close();
    }

    @Before
    public void setUpHttpServer() {
        httpServer.createContext("/", new GoogleCloudStorageHttpHandler("bucket"));
        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
    }

    @After
    public void tearDownHttpServer() {
        httpServer.removeContext("/");
        httpServer.removeContext("/token");
    }

    @Override
    protected String repositoryType() {
        return GoogleCloudStorageRepository.TYPE;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings repositorySettings(String repoName) {
        return Settings.builder()
            .put(super.repositorySettings(repoName))
            .put(BUCKET.getKey(), "bucket")
            .put(CLIENT_NAME.getKey(), "test")
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        var secureSettings = new MockSecureSettings();
        secureSettings.setFile(
            CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace("test").getKey(),
            TestUtils.createServiceAccount(random())
        );
        String upstreamServerUrl = "http://" + httpServer.getAddress().getHostString() + ":" + httpServer.getAddress().getPort();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ENDPOINT_SETTING.getConcreteSettingForNamespace("test").getKey(), upstreamServerUrl)
            .put(TOKEN_URI_SETTING.getConcreteSettingForNamespace("test").getKey(), upstreamServerUrl + "/token")
            .put(PROXY_HOST_SETTING.getConcreteSettingForNamespace("test").getKey(), proxyServer.getHost())
            .put(PROXY_PORT_SETTING.getConcreteSettingForNamespace("test").getKey(), proxyServer.getPort())
            .put(PROXY_TYPE_SETTING.getConcreteSettingForNamespace("test").getKey(), "http")
            .setSecureSettings(secureSettings)
            .build();
    }
}
