/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.cli.SuppressForbidden;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.net.InetSocketAddress;
import java.net.Proxy;

class ProxyMatcher extends TypeSafeMatcher<Proxy> {
    private final Proxy.Type type;
    private final String hostname;
    private final int port;

    public static ProxyMatcher matchesProxy(Proxy.Type type, String hostname, int port) {
        return new ProxyMatcher(type, hostname, port);
    }

    public static ProxyMatcher matchesProxy(Proxy.Type type) {
        return new ProxyMatcher(type, null, -1);
    }

    ProxyMatcher(Proxy.Type type, String hostname, int port) {
        this.type = type;
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    @SuppressForbidden(reason = "Proxy constructor uses InetSocketAddress")
    protected boolean matchesSafely(Proxy proxy) {
        if (proxy.type() != this.type) {
            return false;
        }

        if (hostname == null) {
            return true;
        }

        InetSocketAddress address = (InetSocketAddress) proxy.address();

        return this.hostname.equals(address.getHostName()) && this.port == address.getPort();
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a proxy instance of type [" + type + "] pointing at [" + hostname + ":" + port + "]");
    }
}
