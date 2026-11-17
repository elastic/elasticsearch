/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.impl.bootstrap.HttpServer;
import org.apache.hc.core5.http.impl.bootstrap.ServerBootstrap;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.io.CloseMode;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

/**
 * A mock web proxy server that forwards proxied requests to
 * their target host and records every request it handles, so tests can assert whether traffic was (or was not) routed through the proxy.
 * Each forwarded request is stamped with an {@link #VIA_HEADER} header so the upstream side of the hop can be verified as well.
 *
 * <p>Adapted from the {@code repository-gcs} test fixtures ({@code MockHttpProxyServer} and {@code WebProxyServer}). Only plain HTTP
 * forward-proxying is supported (absolute-URI request lines); {@code CONNECT} tunnel handshakes are recorded but never tunneled: by
 * default they are rejected with {@code 502 Bad Gateway}, so clients that only proxy by tunneling (e.g. the AWS SDK Netty client) can
 * still assert that their traffic was routed to the proxy without any bytes leaving the host. {@link #grantingConnectTunnels()} switches
 * to a {@code 200} response instead, for tests where the client is expected to fail on its own after the handshake. The proxy itself can
 * be served over TLS (the {@code https} proxy scheme) by passing an {@link SSLContext}.
 */
public class MockHttpProxyServer implements Closeable {

    public static final String VIA_HEADER = "X-Via";
    public static final String VIA_HEADER_VALUE = "mock-http-proxy-server";

    /**
     * Hop-by-hop (and hop-managed) headers that must not be copied verbatim between the two legs of the proxied exchange; the forwarding
     * client and the upstream connection manage these themselves.
     */
    private static final Set<String> BLOCKED_HEADERS = Stream.of(
        "Host",
        "Proxy-Connection",
        "Proxy-Authenticate",
        "Content-Length",
        "Transfer-Encoding"
    ).collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

    private final HttpServer httpServer;
    private final CloseableHttpClient forwardingClient = HttpClients.createDefault();
    private final List<String> proxiedRequestLines = new CopyOnWriteArrayList<>();
    private volatile int connectResponseStatus = HttpStatus.SC_BAD_GATEWAY;

    public MockHttpProxyServer() {
        this(null);
    }

    /**
     * With a non-null {@code sslContext} the proxy itself is served over TLS, i.e. clients must TLS-connect to the proxy (the
     * {@code https} proxy scheme) before speaking HTTP to it.
     */
    public MockHttpProxyServer(@Nullable SSLContext sslContext) {
        httpServer = ServerBootstrap.bootstrap()
            .setLocalAddress(InetAddress.getLoopbackAddress())
            .setListenerPort(0)
            .setSslContext(sslContext)
            // route every request to the handler: a forward-proxied request carries the target's authority, which would otherwise
            // be rejected as a misdirected request (421) because no handler is registered for that virtual host
            .setRequestRouter((request, context) -> this::handle)
            .create();
        try {
            httpServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start HTTP proxy server", e);
        }
    }

    /**
     * Respond {@code 200} to {@code CONNECT} handshakes instead of rejecting them. No tunnel is actually established, so this is only
     * suitable for tests where the client is expected to fail on its own before sending bytes through the granted tunnel.
     */
    public MockHttpProxyServer grantingConnectTunnels() {
        connectResponseStatus = HttpStatus.SC_OK;
        return this;
    }

    private void handle(ClassicHttpRequest request, ClassicHttpResponse response, HttpContext context) throws IOException {
        try {
            proxiedRequestLines.add(requestLine(request));

            if ("CONNECT".equalsIgnoreCase(request.getMethod())) {
                // Tunneling is never performed; the handshake is recorded and then rejected (or granted without a tunnel) so the client
                // fails instead of talking to the target
                response.setCode(connectResponseStatus);
                return;
            }

            // A forward proxy receives the full target URI in the request line, which is where we forward the request to
            var upstreamRequest = new HttpUriRequestBase(request.getMethod(), request.getUri());
            upstreamRequest.setHeader(VIA_HEADER, VIA_HEADER_VALUE);
            for (Header requestHeader : request.getHeaders()) {
                if (BLOCKED_HEADERS.contains(requestHeader.getName()) == false) {
                    upstreamRequest.setHeader(requestHeader.getName(), requestHeader.getValue());
                }
            }
            if (request.getEntity() != null) {
                upstreamRequest.setEntity(copyOf(request.getEntity()));
            }
            forwardingClient.execute(upstreamRequest, upstreamResponse -> {
                response.setCode(upstreamResponse.getCode());
                for (Header upstreamHeader : upstreamResponse.getHeaders()) {
                    if (BLOCKED_HEADERS.contains(upstreamHeader.getName()) == false) {
                        response.addHeader(upstreamHeader.getName(), upstreamHeader.getValue());
                    }
                }
                if (upstreamResponse.getEntity() != null) {
                    response.setEntity(copyOf(upstreamResponse.getEntity()));
                }
                return null;
            });
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Reconstructs the request line as the client sent it: the authority for {@code CONNECT} and the absolute target URI otherwise.
     */
    private static String requestLine(ClassicHttpRequest request) throws URISyntaxException {
        var target = "CONNECT".equalsIgnoreCase(request.getMethod()) ? request.getAuthority().toString() : request.getUri().toString();
        return request.getMethod() + " " + target + " HTTP/1.1";
    }

    /** Fully buffers the entity so the originating connection can be released before the entity is replayed on the other leg. */
    private static HttpEntity copyOf(HttpEntity entity) throws IOException {
        var contentType = entity.getContentType() == null ? null : ContentType.parse(entity.getContentType());
        return new ByteArrayEntity(EntityUtils.toByteArray(entity), contentType);
    }

    public int getPort() {
        return httpServer.getLocalPort();
    }

    public String getHost() {
        return NetworkAddress.format(httpServer.getInetAddress());
    }

    /**
     * The request lines (e.g. {@code POST http://127.0.0.1:12345/path HTTP/1.1}) of every request this proxy has handled, in order.
     */
    public List<String> proxiedRequestLines() {
        return List.copyOf(proxiedRequestLines);
    }

    @Override
    public void close() throws IOException {
        forwardingClient.close();
        httpServer.close(CloseMode.GRACEFUL);
    }
}
