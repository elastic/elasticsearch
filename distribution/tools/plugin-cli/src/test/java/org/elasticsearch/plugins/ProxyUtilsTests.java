/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.net.Proxy.Type;
import java.util.stream.Stream;

import static org.elasticsearch.plugins.ProxyMatcher.matchesProxy;
import static org.hamcrest.Matchers.equalTo;

public class ProxyUtilsTests extends ESTestCase {
    /**
     * Check that building a proxy with just a hostname and port succeeds.
     */
    public void testBuildProxy_withHostPort() throws UserException {
        assertThat(ProxyUtils.buildProxy("host:1234"), matchesProxy(Type.HTTP, "host", 1234));
    }

    /**
     * Check that building a proxy with a hostname with domain and a port succeeds.
     */
    public void testBuildProxy_withHostDomainPort() throws UserException {
        assertThat(ProxyUtils.buildProxy("host.localhost:1234"), matchesProxy(Type.HTTP, "host.localhost", 1234));
    }

    /**
     * Check that building a proxy with a null value succeeds, returning a pass-through (direct) proxy.
     */
    public void testBuildProxy_withNullValue() throws UserException {
        assertThat(ProxyUtils.buildProxy(null), matchesProxy(Type.DIRECT));
    }

    /**
     * Check that building a proxy with an invalid host is rejected.
     */
    public void testBuildProxy_withInvalidHost() {
        Stream.of("blah_blah:1234", "-host.domain:1234", "host.-domain:1234", "tést:1234", ":1234").forEach(testCase -> {
            UserException e = expectThrows(UserException.class, () -> ProxyUtils.buildProxy(testCase));
            assertThat(e.getMessage(), equalTo("Malformed [proxy], expected [host:port]"));
        });
    }

    /**
     * Check that building a proxy with an invalid port is rejected.
     */
    public void testBuildProxy_withInvalidPort() {
        Stream.of("host.domain:-1", "host.domain:$PORT", "host.domain:{{port}}", "host.domain").forEach(testCase -> {
            UserException e = expectThrows(UserException.class, () -> ProxyUtils.buildProxy(testCase));
            assertThat(e.getMessage(), equalTo("Malformed [proxy], expected [host:port]"));
        });
    }

    /**
     * Check that building a proxy with a null input but with system {@code http.*} properties set returns the correct proxy.
     */
    @SuppressForbidden(reason = "Sets http proxy properties")
    public void testBuildProxy_withNullValueAndSystemHttpProxy() throws UserException {
        String prevHost = null;
        String prevPort = null;

        try {
            prevHost = System.getProperty("http.proxyHost");
            prevPort = System.getProperty("http.proxyPort");
            System.setProperty("http.proxyHost", "host.localhost");
            System.setProperty("http.proxyPort", "1234");

            assertThat(ProxyUtils.buildProxy(null), matchesProxy(Type.HTTP, "host.localhost", 1234));
        } finally {
            System.setProperty("http.proxyHost", prevHost == null ? "" : prevHost);
            System.setProperty("http.proxyPort", prevPort == null ? "" : prevPort);
        }
    }

    /**
     * Check that building a proxy with a null input but with system {@code https.*} properties set returns the correct proxy.
     */
    @SuppressForbidden(reason = "Sets https proxy properties")
    public void testBuildProxy_withNullValueAndSystemHttpsProxy() throws UserException {
        String prevHost = null;
        String prevPort = null;

        try {
            prevHost = System.getProperty("https.proxyHost");
            prevPort = System.getProperty("https.proxyPort");
            System.setProperty("https.proxyHost", "host.localhost");
            System.setProperty("https.proxyPort", "1234");

            assertThat(ProxyUtils.buildProxy(null), matchesProxy(Type.HTTP, "host.localhost", 1234));
        } finally {
            System.setProperty("https.proxyHost", prevHost == null ? "" : prevHost);
            System.setProperty("https.proxyPort", prevPort == null ? "" : prevPort);
        }
    }

    /**
     * Check that building a proxy with a null input but with system {@code socks.*} properties set returns the correct proxy.
     */
    @SuppressForbidden(reason = "Sets socks proxy properties")
    public void testBuildProxy_withNullValueAndSystemSocksProxy() throws UserException {
        String prevHost = null;
        String prevPort = null;

        try {
            prevHost = System.getProperty("socks.proxyHost");
            prevPort = System.getProperty("socks.proxyPort");
            System.setProperty("socks.proxyHost", "host.localhost");
            System.setProperty("socks.proxyPort", "1234");

            assertThat(ProxyUtils.buildProxy(null), matchesProxy(Type.SOCKS, "host.localhost", 1234));
        } finally {
            System.setProperty("socks.proxyHost", prevHost == null ? "" : prevHost);
            System.setProperty("socks.proxyPort", prevPort == null ? "" : prevPort);
        }
    }
}
