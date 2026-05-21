/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.instrumentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class HttpServerSemConvAttributesTests extends ESTestCase {

    // ---- parsePortSafe ----

    public void testParsePortSafe_validPorts() {
        assertThat(HttpServerSemConvAttributes.parsePortSafe("1"), equalTo(1));
        assertThat(HttpServerSemConvAttributes.parsePortSafe("80"), equalTo(80));
        assertThat(HttpServerSemConvAttributes.parsePortSafe("65535"), equalTo(65535));
    }

    public void testParsePortSafe_invalidPorts() {
        assertThat(HttpServerSemConvAttributes.parsePortSafe(null), nullValue());
        assertThat(HttpServerSemConvAttributes.parsePortSafe("0"), nullValue());
        assertThat(HttpServerSemConvAttributes.parsePortSafe("65536"), nullValue());
        assertThat(HttpServerSemConvAttributes.parsePortSafe("-1"), nullValue());
        assertThat(HttpServerSemConvAttributes.parsePortSafe("abc"), nullValue());
        assertThat(HttpServerSemConvAttributes.parsePortSafe(""), nullValue());
    }

    public void testParsePortSafe_stripsWhitespace() {
        assertThat(HttpServerSemConvAttributes.parsePortSafe("  8080  "), equalTo(8080));
    }

    // ---- splitHostPort ----

    public void testSplitHostPort_plainHost() {
        String[] result = HttpServerSemConvAttributes.splitHostPort("example.com");
        assertThat(result[0], equalTo("example.com"));
        assertThat(result[1], nullValue());
    }

    public void testSplitHostPort_hostWithPort() {
        String[] result = HttpServerSemConvAttributes.splitHostPort("example.com:8080");
        assertThat(result[0], equalTo("example.com"));
        assertThat(result[1], equalTo("8080"));
    }

    public void testSplitHostPort_ipv4WithPort() {
        String[] result = HttpServerSemConvAttributes.splitHostPort("192.168.1.1:443");
        assertThat(result[0], equalTo("192.168.1.1"));
        assertThat(result[1], equalTo("443"));
    }

    public void testSplitHostPort_ipv6Bracketed() {
        String[] result = HttpServerSemConvAttributes.splitHostPort("[::1]");
        assertThat(result[0], equalTo("::1"));
        assertThat(result[1], nullValue());
    }

    public void testSplitHostPort_ipv6BracketedWithPort() {
        String[] result = HttpServerSemConvAttributes.splitHostPort("[::1]:8080");
        assertThat(result[0], equalTo("::1"));
        assertThat(result[1], equalTo("8080"));
    }

    // ---- extractForwardedParam ----

    public void testExtractForwardedParam_simpleProto() {
        assertThat(HttpServerSemConvAttributes.extractForwardedParam("proto=https", "proto"), equalTo("https"));
    }

    public void testExtractForwardedParam_quotedValue() {
        assertThat(HttpServerSemConvAttributes.extractForwardedParam("proto=\"https\"", "proto"), equalTo("https"));
    }

    public void testExtractForwardedParam_multipleParams() {
        assertThat(HttpServerSemConvAttributes.extractForwardedParam("host=proxy.example.com;proto=https", "proto"), equalTo("https"));
        assertThat(
            HttpServerSemConvAttributes.extractForwardedParam("host=proxy.example.com;proto=https", "host"),
            equalTo("proxy.example.com")
        );
    }

    public void testExtractForwardedParam_missingParam() {
        assertThat(HttpServerSemConvAttributes.extractForwardedParam("host=proxy.example.com", "proto"), nullValue());
    }

    public void testExtractForwardedParam_caseInsensitive() {
        assertThat(HttpServerSemConvAttributes.extractForwardedParam("Proto=https", "proto"), equalTo("https"));
    }

    // ---- extractScheme ----

    public void testExtractScheme_fromForwardedProto() {
        FakeRestRequest req = requestWithHeaders(Map.of("Forwarded", List.of("proto=https"), "X-Forwarded-Proto", List.of("http")));
        assertThat(HttpServerSemConvAttributes.extractScheme(req, "proto=https"), equalTo("https"));
    }

    public void testExtractScheme_fromXForwardedProto() {
        FakeRestRequest req = requestWithHeaders(Map.of("X-Forwarded-Proto", List.of("https")));
        assertThat(HttpServerSemConvAttributes.extractScheme(req, null), equalTo("https"));
    }

    public void testExtractScheme_fallbackToTransportScheme() {
        FakeRestRequest req = requestWithHeaders(Map.of());
        // TestHttpRequest.getScheme() returns "http" by default
        assertThat(HttpServerSemConvAttributes.extractScheme(req, null), equalTo("http"));
    }

    // ---- extractServerAddress ----

    public void testExtractServerAddress_fromForwardedHost() {
        FakeRestRequest req = requestWithHeaders(Map.of("Forwarded", List.of("host=proxy.example.com"), "Host", List.of("origin.com")));
        String addr = HttpServerSemConvAttributes.extractServerAddress(req, null, "host=proxy.example.com");
        assertThat(addr, equalTo("proxy.example.com"));
    }

    public void testExtractServerAddress_fromXForwardedHost() {
        FakeRestRequest req = requestWithHeaders(Map.of("X-Forwarded-Host", List.of("proxy.example.com"), "Host", List.of("origin.com")));
        String addr = HttpServerSemConvAttributes.extractServerAddress(req, null, null);
        assertThat(addr, equalTo("proxy.example.com"));
    }

    public void testExtractServerAddress_fromHostHeader() {
        FakeRestRequest req = requestWithHeaders(Map.of("Host", List.of("origin.com:9200")));
        String addr = HttpServerSemConvAttributes.extractServerAddress(req, null, null);
        assertThat(addr, equalTo("origin.com"));
    }

    public void testExtractServerAddress_fromLocalSocket() {
        FakeRestRequest req = requestWithHeaders(Map.of());
        HttpChannel channel = localAddressChannel(new InetSocketAddress(addr192_168_0_1(), 9200));
        String addr = HttpServerSemConvAttributes.extractServerAddress(req, channel, null);
        assertThat(addr, equalTo("192.168.0.1"));
    }

    public void testExtractServerAddress_nullWhenNoSource() {
        FakeRestRequest req = requestWithHeaders(Map.of());
        String addr = HttpServerSemConvAttributes.extractServerAddress(req, null, null);
        assertThat(addr, nullValue());
    }

    // ---- extractServerPort ----

    public void testExtractServerPort_fromForwardedHostWithPort() {
        FakeRestRequest req = requestWithHeaders(Map.of("Forwarded", List.of("host=proxy.example.com:8443")));
        Integer port = HttpServerSemConvAttributes.extractServerPort(req, null, "host=proxy.example.com:8443");
        assertThat(port, equalTo(8443));
    }

    public void testExtractServerPort_fromXForwardedPort() {
        FakeRestRequest req = requestWithHeaders(
            Map.of("X-Forwarded-Host", List.of("proxy.example.com"), "X-Forwarded-Port", List.of("8443"))
        );
        Integer port = HttpServerSemConvAttributes.extractServerPort(req, null, null);
        assertThat(port, equalTo(8443));
    }

    public void testExtractServerPort_fromHostHeader() {
        FakeRestRequest req = requestWithHeaders(Map.of("Host", List.of("origin.com:9200")));
        Integer port = HttpServerSemConvAttributes.extractServerPort(req, null, null);
        assertThat(port, equalTo(9200));
    }

    public void testExtractServerPort_fromLocalSocket() {
        FakeRestRequest req = requestWithHeaders(Map.of());
        HttpChannel channel = localAddressChannel(new InetSocketAddress(addr192_168_0_1(), 9200));
        Integer port = HttpServerSemConvAttributes.extractServerPort(req, channel, null);
        assertThat(port, equalTo(9200));
    }

    public void testExtractServerPort_nullWhenNoSource() {
        FakeRestRequest req = requestWithHeaders(Map.of());
        Integer port = HttpServerSemConvAttributes.extractServerPort(req, null, null);
        assertThat(port, nullValue());
    }

    // ---- helpers ----

    private FakeRestRequest requestWithHeaders(Map<String, List<String>> headers) {
        return new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();
    }

    private static InetAddress addr192_168_0_1() {
        try {
            return InetAddress.getByAddress(new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    private static HttpChannel localAddressChannel(InetSocketAddress local) {
        return new HttpChannel() {
            @Override
            public void sendResponse(HttpResponse response, ActionListener<Void> listener) {}

            @Override
            public InetSocketAddress getLocalAddress() {
                return local;
            }

            @Override
            public InetSocketAddress getRemoteAddress() {
                return null;
            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {}

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() {}
        };
    }
}
