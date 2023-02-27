/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Emulates a <a href="https://en.wikipedia.org/wiki/Proxy_server#Web_proxy_servers">Web Proxy Server</a>
 */
class WebProxyServer extends MockHttpProxyServer {

    private static final Set<String> BLOCKED_HEADERS = Stream.of("Host", "Proxy-Connection", "Proxy-Authenticate")
        .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

    WebProxyServer() throws IOException {
        super(WebProxyServer::handle);
    }

    @SuppressForbidden(reason = "Proxy makes requests to the upstream HTTP server")
    private static void handle(InputStream is, OutputStream os) throws IOException {
        // We can't make a com.sun.net.httpserver act as an HTTP proxy, so we have to do work with
        // raw sockets and do HTTP parsing ourselves
        String requestLine = readLine(is);
        String[] parts = requestLine.split(" ");
        String requestMethod = parts[0];
        String originUrl = parts[1];

        var upstreamHttpConnection = (HttpURLConnection) URI.create(originUrl).toURL().openConnection();
        upstreamHttpConnection.setRequestMethod(requestMethod);
        upstreamHttpConnection.setRequestProperty("X-Via", "test-web-proxy-server");

        int requestContentLength = -1;
        boolean chunkedRequest = false;
        while (true) {
            String requestHeader = readLine(is);
            if (requestHeader.isEmpty()) {
                // End of the headers block
                break;
            }
            String[] headerParts = requestHeader.split(":");
            String headerName = headerParts[0].trim();
            String headerValue = headerParts[1].trim();
            if (headerName.equalsIgnoreCase("Content-Length")) {
                requestContentLength = Integer.parseInt(headerValue);
            } else if (headerName.equalsIgnoreCase("Transfer-Encoding") && headerValue.equalsIgnoreCase("chunked")) {
                chunkedRequest = true;
            }
            if (BLOCKED_HEADERS.contains(headerName) == false) {
                upstreamHttpConnection.setRequestProperty(headerName, headerValue);
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
                    int chunkSize = Integer.parseInt(readLine(is), 16);
                    if (chunkSize == 0) {
                        // End of the chunked body
                        break;
                    }
                    uos.write(is.readNBytes(chunkSize));
                    if (is.read() != '\r' || is.read() != '\n') {
                        throw new IllegalStateException("Not CRLF");
                    }
                }
            }
        }
        upstreamHttpConnection.connect();

        String upstreamStatusLine = Strings.format(
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
        // HttpURLConnection handles chunked and fixed-length responses transparently
        try (var uis = upstreamHttpConnection.getInputStream()) {
            uis.transferTo(os);
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
                if (is.read() != '\n') {
                    throw new IllegalStateException("Not CRLF");
                }
                break;
            }
            os.write(b);
        }
        return os.toString(ISO_8859_1);
    }
}
