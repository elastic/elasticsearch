/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.instrumentation;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.RestRequest;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;

/**
 * Stateless utilities for resolving OTel HTTP server SemConv attributes that require
 * proxy-header awareness. The priority order follows RFC 7239 and de-facto standards:
 * <ol>
 *   <li>{@code Forwarded} (RFC 7239)</li>
 *   <li>{@code X-Forwarded-Proto} / {@code X-Forwarded-Host} / {@code X-Forwarded-Port}</li>
 *   <li>{@code Host} header</li>
 *   <li>local socket address (fallback)</li>
 * </ol>
 * These helpers are package-visible so that {@code HttpServerInstrumentation} implementations
 * in other modules (e.g. APM) can import and call them without duplicating the logic.
 */
public final class HttpServerSemConvAttributes {

    private HttpServerSemConvAttributes() {}

    /**
     * Resolves {@code url.scheme} (OTel HTTP server SemConv).
     * Checks {@code Forwarded proto=} (RFC 7239), then {@code X-Forwarded-Proto},
     * then falls back to the transport-layer scheme from {@link org.elasticsearch.http.HttpRequest#getScheme()}.
     *
     * @param req       the REST request
     * @param forwarded the first value of the {@code Forwarded} request header, or {@code null}
     */
    public static String extractScheme(RestRequest req, String forwarded) {
        if (forwarded != null) {
            final String proto = extractForwardedParam(forwarded, "proto");
            if (proto != null) {
                return proto.toLowerCase(Locale.ROOT);
            }
        }
        final String xProto = firstHeaderValue(req, "X-Forwarded-Proto");
        if (xProto != null && xProto.isBlank() == false) {
            return xProto.strip().toLowerCase(Locale.ROOT);
        }
        return req.getHttpRequest().getScheme();
    }

    /**
     * Resolves {@code server.address} (OTel HTTP server SemConv).
     * Checks {@code Forwarded host=} (RFC 7239), then {@code X-Forwarded-Host},
     * then {@code Host} header, then the local socket address.
     *
     * @param req         the REST request
     * @param httpChannel the HTTP channel (used for local socket fallback); may be {@code null}
     * @param forwarded   the first value of the {@code Forwarded} request header, or {@code null}
     */
    public static String extractServerAddress(RestRequest req, HttpChannel httpChannel, String forwarded) {
        if (forwarded != null) {
            final String host = extractForwardedParam(forwarded, "host");
            if (host != null) {
                final String address = splitHostPort(host)[0];
                if (address != null) return address;
            }
        }
        final String xHost = firstHeaderValue(req, "X-Forwarded-Host");
        if (xHost != null && xHost.isBlank() == false) {
            final String address = splitHostPort(xHost.strip())[0];
            if (address != null) return address;
        }
        final String hostHeader = firstHeaderValue(req, "Host");
        if (hostHeader != null && hostHeader.isBlank() == false) {
            final String address = splitHostPort(hostHeader.strip())[0];
            if (address != null) return address;
        }
        if (httpChannel != null) {
            final InetSocketAddress local = httpChannel.getLocalAddress();
            if (local != null) return NetworkAddress.format(local.getAddress());
        }
        return null;
    }

    /**
     * Resolves {@code server.port} (OTel HTTP server SemConv).
     * Checks {@code Forwarded host=} (RFC 7239), then {@code X-Forwarded-Host}/{@code X-Forwarded-Port},
     * then the {@code Host} header, then the local socket port.
     * <p>
     * When an address comes from a forwarding header without a port component, no fallback to the
     * socket port is attempted. {@code X-Forwarded-Port} is only consulted when {@code X-Forwarded-Host}
     * is also present; a standalone {@code X-Forwarded-Port} is ignored.
     *
     * @param req         the REST request
     * @param httpChannel the HTTP channel (used for local socket fallback); may be {@code null}
     * @param forwarded   the first value of the {@code Forwarded} request header, or {@code null}
     */
    public static Integer extractServerPort(RestRequest req, HttpChannel httpChannel, String forwarded) {
        if (forwarded != null) {
            final String host = extractForwardedParam(forwarded, "host");
            if (host != null) {
                final String[] addrPort = splitHostPort(host);
                if (addrPort[0] != null) return parsePortSafe(addrPort[1]);
            }
        }
        final String xHost = firstHeaderValue(req, "X-Forwarded-Host");
        if (xHost != null && xHost.isBlank() == false) {
            final String[] addrPort = splitHostPort(xHost.strip());
            if (addrPort[0] != null) {
                final String xPort = firstHeaderValue(req, "X-Forwarded-Port");
                return parsePortSafe(xPort != null ? xPort.strip() : addrPort[1]);
            }
        }
        final String hostHeader = firstHeaderValue(req, "Host");
        if (hostHeader != null && hostHeader.isBlank() == false) {
            final String[] addrPort = splitHostPort(hostHeader.strip());
            if (addrPort[0] != null) return parsePortSafe(addrPort[1]);
        }
        if (httpChannel != null) {
            final InetSocketAddress local = httpChannel.getLocalAddress();
            if (local != null) return local.getPort();
        }
        return null;
    }

    /** Returns the first value of the named request header, or {@code null} if absent. */
    public static String firstHeaderValue(RestRequest req, String name) {
        final List<String> values = req.getHeaders().get(name);
        return values != null && values.isEmpty() == false ? values.get(0) : null;
    }

    /**
     * Extracts the value of the named directive from an RFC 7239 {@code Forwarded} header value.
     * Handles quoted values. Note: quoted values containing {@code ';'} or {@code ','} are not
     * fully supported (the split will produce incorrect tokens).
     */
    public static String extractForwardedParam(String forwarded, String param) {
        final String prefix = param + "=";
        for (String entry : forwarded.split(",")) {
            for (String directive : entry.split(";")) {
                final String token = directive.strip();
                if (token.regionMatches(true, 0, prefix, 0, prefix.length())) {
                    String value = token.substring(prefix.length()).strip();
                    if (value.length() > 1 && value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    if (value.isBlank() == false) {
                        return value;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Splits a {@code host[:port]} token into a two-element {@code {address, port}} array.
     * The port element may be {@code null}. Handles IPv6 bracketed addresses ({@code [::1]:8080}).
     */
    public static String[] splitHostPort(String hostToken) {
        if (hostToken.startsWith("[")) {
            final int close = hostToken.indexOf(']');
            if (close < 0) {
                return new String[] { hostToken, null };
            }
            final String address = hostToken.substring(1, close);
            final String rest = hostToken.substring(close + 1);
            return new String[] { address.isBlank() ? null : address, rest.startsWith(":") ? rest.substring(1) : null };
        }
        final int colon = hostToken.lastIndexOf(':');
        if (colon < 0) {
            return new String[] { hostToken.isBlank() ? null : hostToken, null };
        }
        final String address = hostToken.substring(0, colon);
        return new String[] { address.isBlank() ? null : address, hostToken.substring(colon + 1) };
    }

    /**
     * Parses a port string. Returns {@code null} if {@code portStr} is {@code null}, not a valid
     * integer, or outside the valid port range {@code [1, 65535]}.
     */
    public static Integer parsePortSafe(String portStr) {
        if (portStr == null) return null;
        try {
            final int port = Integer.parseInt(portStr.strip());
            return port > 0 && port <= 65535 ? port : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
