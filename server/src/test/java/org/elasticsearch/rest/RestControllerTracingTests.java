/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class RestControllerTracingTests extends ESTestCase {

    public void testParsePortSafeNull() {
        assertNull(RestController.parsePortSafe(null));
    }

    public void testParsePortSafeNonInteger() {
        assertNull(RestController.parsePortSafe("abc"));
    }

    public void testParsePortSafeBoundaries() {
        assertNull(RestController.parsePortSafe("0"));
        assertEquals(Integer.valueOf(1), RestController.parsePortSafe("1"));
        assertEquals(Integer.valueOf(65535), RestController.parsePortSafe("65535"));
        assertNull(RestController.parsePortSafe("65536"));
        assertNull(RestController.parsePortSafe("-1"));
    }

    public void testParsePortSafeTrimsWhitespace() {
        assertEquals(Integer.valueOf(8080), RestController.parsePortSafe(" 8080 "));
    }

    public void testSplitHostPortIpv4WithPort() {
        final var result = RestController.splitHostPort("192.168.1.1:8080");
        assertEquals("192.168.1.1", result[0]);
        assertEquals("8080", result[1]);
    }

    public void testSplitHostPortNameWithPort() {
        final var result = RestController.splitHostPort("example.com:9200");
        assertEquals("example.com", result[0]);
        assertEquals("9200", result[1]);
    }

    public void testSplitHostPortNameWithoutPort() {
        final var result = RestController.splitHostPort("example.com");
        assertEquals("example.com", result[0]);
        assertNull(result[1]);
    }

    public void testSplitHostPortIpv6WithPort() {
        final var result = RestController.splitHostPort("[::1]:8080");
        assertEquals("::1", result[0]);
        assertEquals("8080", result[1]);
    }

    public void testSplitHostPortIpv6WithoutPort() {
        final var result = RestController.splitHostPort("[::1]");
        assertEquals("::1", result[0]);
        assertNull(result[1]);
    }

    public void testSplitHostPortEmpty() {
        final var result = RestController.splitHostPort("");
        assertNull(result[0]);
        assertNull(result[1]);
    }

    public void testSplitHostPortMalformedMissingAddress() {
        final var result = RestController.splitHostPort(":8443");
        assertNull(result[0]);
        assertEquals("8443", result[1]);
    }

    public void testExtractForwardedParamSimple() {
        assertEquals("https", RestController.extractForwardedParam("proto=https", "proto"));
    }

    public void testExtractForwardedParamCaseInsensitiveKey() {
        assertEquals("https", RestController.extractForwardedParam("PROTO=https", "proto"));
    }

    public void testExtractForwardedParamAmongSemicolonDirectives() {
        assertEquals("https", RestController.extractForwardedParam("for=1.2.3.4;proto=https", "proto"));
    }

    public void testExtractForwardedParamMultiHopComma() {
        assertEquals("https", RestController.extractForwardedParam("for=1.2.3.4, proto=https", "proto"));
    }

    public void testExtractForwardedParamQuotedValue() {
        assertEquals("proxy.com:8443", RestController.extractForwardedParam("host=\"proxy.com:8443\"", "host"));
    }

    public void testExtractForwardedParamAbsent() {
        assertNull(RestController.extractForwardedParam("for=1.2.3.4", "proto"));
    }

    public void testExtractForwardedParamEmptyValue() {
        assertNull(RestController.extractForwardedParam("proto=", "proto"));
    }

    public void testExtractSchemeFromForwarded() {
        final var req = requestWith(Map.of("Forwarded", List.of("proto=https")));
        assertEquals("https", RestController.extractScheme(req, forwarded(req)));
    }

    public void testExtractSchemeForwardedNormalizesCase() {
        final var req = requestWith(Map.of("Forwarded", List.of("proto=HTTPS")));
        assertEquals("https", RestController.extractScheme(req, forwarded(req)));
    }

    public void testExtractSchemeForwardedBeatsXForwardedProto() {
        final var req = requestWith(Map.of("Forwarded", List.of("proto=https"), "X-Forwarded-Proto", List.of("http")));
        assertEquals("https", RestController.extractScheme(req, forwarded(req)));
    }

    public void testExtractSchemeForwardedWithoutProtoFallsThrough() {
        final var req = requestWith(Map.of("Forwarded", List.of("for=1.2.3.4"), "X-Forwarded-Proto", List.of("https")));
        assertEquals("https", RestController.extractScheme(req, forwarded(req)));
    }

    public void testExtractSchemeFromXForwardedProto() {
        final var req = requestWith(Map.of("X-Forwarded-Proto", List.of("https")));
        assertEquals("https", RestController.extractScheme(req, null));
    }

    public void testExtractSchemeBlankXForwardedProtoFallsThrough() {
        final var req = requestWith(Map.of("X-Forwarded-Proto", List.of("  ")));
        assertEquals("http", RestController.extractScheme(req, null));
    }

    public void testExtractSchemeFallsBackToTransportScheme() {
        assertEquals("http", RestController.extractScheme(requestWith(Map.of()), null));
        final var httpsReq = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHttps(true).build();
        assertEquals("https", RestController.extractScheme(httpsReq, null));
    }

    public void testExtractServerAddressFromForwarded() {
        final var req = requestWith(Map.of("Forwarded", List.of("host=proxy.com:8443")));
        assertEquals("proxy.com", RestController.extractServerAddress(req, null, forwarded(req)));
    }

    public void testExtractServerAddressForwardedBeatsXForwardedHost() {
        final var req = requestWith(Map.of("Forwarded", List.of("host=proxy.com"), "X-Forwarded-Host", List.of("other.com")));
        assertEquals("proxy.com", RestController.extractServerAddress(req, null, forwarded(req)));
    }

    public void testExtractServerAddressFromXForwardedHost() {
        final var req = requestWith(Map.of("X-Forwarded-Host", List.of("proxy.com:8443")));
        assertEquals("proxy.com", RestController.extractServerAddress(req, null, null));
    }

    public void testExtractServerAddressFromHostHeader() {
        final var req = requestWith(Map.of("Host", List.of("example.com:9200")));
        assertEquals("example.com", RestController.extractServerAddress(req, null, null));
    }

    public void testExtractServerAddressFromSocket() {
        final var channel = new FakeRestRequest.FakeHttpChannel(null, new InetSocketAddress("127.0.0.1", 9200));
        assertEquals("127.0.0.1", RestController.extractServerAddress(requestWith(Map.of()), channel, null));
    }

    public void testExtractServerAddressNullWhenNoSource() {
        assertNull(RestController.extractServerAddress(requestWith(Map.of()), null, null));
    }

    public void testExtractServerPortFromForwarded() {
        final var req = requestWith(Map.of("Forwarded", List.of("host=proxy.com:8443")));
        assertEquals(Integer.valueOf(8443), RestController.extractServerPort(req, null, forwarded(req)));
    }

    public void testExtractServerPortForwardedHostWithoutPortIsNull() {
        final var req = requestWith(Map.of("Forwarded", List.of("host=proxy.com")));
        assertNull(RestController.extractServerPort(req, null, forwarded(req)));
    }

    public void testForwardedHostWithoutPortAddressResolvedPortNotFallenBackToSocket() {
        // Forwarded: host=proxy.com (no port) — address resolves, but socket port must NOT be used as fallback.
        final var channel = new FakeRestRequest.FakeHttpChannel(null, new InetSocketAddress("127.0.0.1", 9200));
        final var req = requestWith(Map.of("Forwarded", List.of("host=proxy.com")));
        final var fwd = forwarded(req);
        assertEquals("proxy.com", RestController.extractServerAddress(req, channel, fwd));
        assertNull(RestController.extractServerPort(req, channel, fwd));
    }

    public void testExtractServerPortFromXForwardedHostWithPort() {
        final var req = requestWith(Map.of("X-Forwarded-Host", List.of("proxy.com:8443")));
        assertEquals(Integer.valueOf(8443), RestController.extractServerPort(req, null, null));
    }

    public void testExtractServerPortXForwardedPortOverridesPortInXForwardedHost() {
        final var req = requestWith(Map.of("X-Forwarded-Host", List.of("proxy.com:9999"), "X-Forwarded-Port", List.of("8443")));
        assertEquals(Integer.valueOf(8443), RestController.extractServerPort(req, null, null));
    }

    public void testExtractServerPortFromXForwardedPortWhenXForwardedHostHasNoPort() {
        final var req = requestWith(Map.of("X-Forwarded-Host", List.of("proxy.com"), "X-Forwarded-Port", List.of("8443")));
        assertEquals(Integer.valueOf(8443), RestController.extractServerPort(req, null, null));
    }

    public void testXForwardedPortWithoutXForwardedHostIsIgnored() {
        final var req = requestWith(Map.of("X-Forwarded-Port", List.of("8443")));
        assertNull(RestController.extractServerPort(req, null, null));
    }

    public void testExtractServerPortFromHostHeader() {
        final var req = requestWith(Map.of("Host", List.of("example.com:9200")));
        assertEquals(Integer.valueOf(9200), RestController.extractServerPort(req, null, null));
    }

    public void testExtractServerPortFromSocket() {
        final var channel = new FakeRestRequest.FakeHttpChannel(null, new InetSocketAddress("127.0.0.1", 9200));
        assertEquals(Integer.valueOf(9200), RestController.extractServerPort(requestWith(Map.of()), channel, null));
    }

    public void testExtractServerPortNullWhenNoSource() {
        assertNull(RestController.extractServerPort(requestWith(Map.of()), null, null));
    }

    private static RestRequest requestWith(Map<String, List<String>> headers) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHeaders(headers).build();
    }

    private static String forwarded(RestRequest req) {
        return RestController.firstHeaderValue(req, "Forwarded");
    }
}
